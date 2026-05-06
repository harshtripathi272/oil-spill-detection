# VesselWatch: Oil Spill Detection System

**VesselWatch** is a production-grade, end-to-end environmental monitoring system that detects and validates potential oil spills by fusing real-time vessel traffic (AIS) data with satellite Synthetic Aperture Radar (SAR) imagery. The system ingests live ship positions from global AIS streams, runs them through a deep-learning anomaly detector, and automatically triggers Sentinel-1 SAR satellite validation via Apache Airflow — enabling rapid, all-weather response to maritime environmental threats.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Kafka Topic Contract](#kafka-topic-contract)
4. [Services](#services)
5. [ML Models](#ml-models)
6. [Dataset & Training Pipeline](#dataset--training-pipeline)
7. [Orchestration (Airflow)](#orchestration-airflow)
8. [Project Structure](#project-structure)
9. [Quick Start](#quick-start)
10. [Environment Configuration](#environment-configuration)
11. [Infrastructure](#infrastructure)
12. [Development Roadmap](#development-roadmap)

---

## System Overview

VesselWatch operates as a decoupled, event-driven pipeline with five major stages:

```
AIS WebSocket (aisstream.io)
  → [Kafka] ais.raw.position_reports
  → Stream Processor
  → [Kafka] ais.cleaned.position_reports + ais.features.vessel_tracks
  → Anomaly Detector  (Transformer Contrastive Encoder + FAISS Memory Bank)
  → [Kafka] ais.anomalies.events
  → Trigger Bridge    (score threshold + geofence + cooldown)
  → [Kafka] sar.trigger.events
  → Airflow DAG       (Kafka Sensor → Sentinel Search → Download → YOLOv8 Inference)
  → Incident State Store (STATE_PROCESSING → STATE_VERIFIED / STATE_FAILED)
```

---

## Architecture

### 1. AIS Ingestion Layer — `ingestion/ais_stream/`

Connects to the `aisstream.io` WebSocket API and publishes raw AIS `PositionReport` messages to Kafka.

| File | Purpose |
|---|---|
| `run_ingestion.py` | Main entry point. Manages WebSocket connection lifecycle, subscribes with configurable bounding boxes, routes valid `PositionReport` messages to Kafka and malformed ones to the dead-letter queue. |
| `run_http_ingestion.py` | Alternative HTTP polling ingestion for REST-based AIS providers. |
| `run_file_ingestion.py` | Offline replay ingestion from local files (for testing / backfill). |
| `ais_ingestion.py` | `AISProducerWrapper` — Kafka producer that serialises and publishes raw AIS records to `ais.raw.position_reports`. |
| `dead_letter/invalid_messages.py` | `DeadLetterHandler` — Publishes unprocessable messages to `ais.deadletter` with an error reason. |

**Design invariant:** `ais.raw.position_reports` is immutable and append-only. No transformation happens in the ingestion layer.

---

### 2. Stream Processor — `services/stream_processor/`

A stateful Kafka consumer-producer that cleans raw AIS events and computes kinematic vessel features.

| File | Purpose |
|---|---|
| `main.py` | Entry point with graceful signal handling. Runs the consume → validate → publish → feature-engineer loop. |
| `config.py` | `StreamProcessorConfig` — all settings sourced from environment variables. |
| `processing.py` | `validate_and_normalize()` — validates coordinate ranges, normalises timestamps. `build_feature_event()` — computes speed, acceleration, heading change rate from vessel state history. |
| `vessel_state.py` | `VesselStateManager` — per-MMSI sliding window of recent positions, backed by **Redis** (with in-memory fallback). Configurable window size (`AIS_VESSEL_WINDOW_SIZE`, default 20) and TTL (`AIS_VESSEL_STATE_TTL_SEC`, default 86400s). |

**Input topic:** `ais.raw.position_reports`  
**Output topics:**
- `ais.cleaned.position_reports` — normalised, validated records
- `ais.features.vessel_tracks` — enriched records with kinematic features

Voyage gap detection: if a vessel's position gap exceeds `AIS_VOYAGE_GAP_HOURS` (default 2h), the trajectory window is reset.

---

### 3. Anomaly Detector — `services/anomaly_detector/`

A model-based service that encodes live vessel trajectory windows with a trained Transformer encoder and scores them against a pre-built embedding memory bank.

| File | Purpose |
|---|---|
| `main.py` | Entry point. Runs inference per feature event, filters below threshold, publishes anomaly events, routes invalid events to DLQ. |
| `config.py` | `AnomalyDetectorConfig` — threshold, checkpoint path, memory bank dir, K-neighbors, etc. |
| `model.py` | `AISRealtimeMemoryBankModel` — wraps the encoder and memory bank for live inference. `build_anomaly_event()` — constructs the schema-versioned anomaly event payload. |

**Scoring dimensions (combined hierarchical score):**
| Score | Weight | What it detects |
|---|---|---|
| **Physics score** | 20% | Kinematic impossibilities (too-short voyages, too few steps) |
| **Global score** | 40% | Distance to nearest K embeddings in the full global memory bank |
| **Local score** | 20% | Distance to nearest K embeddings for the vessel's 1°×1° grid cell |
| **Vessel score** | 20% | Mahalanobis distance from vessel's own historical embedding distribution (falls back to vessel-type cohort for unknown MMSIs) |

**Output topic:** `ais.anomalies.events`

**Anomaly event schema:**
```json
{
  "schema_version": "1.0",
  "event_id": "<uuid>",
  "vessel_id": "<mmsi>",
  "lat": 0.0,
  "lon": 0.0,
  "timestamp": "2026-05-06T10:30:00Z",
  "anomaly_type": "model_detected",
  "score": 0.87,
  "model": {
    "name": "AIS-Contrastive-Encoder-v1",
    "label": "anomalous"
  },
  "features": {}
}
```

---

### 4. Trigger Bridge — `services/trigger_bridge/`

Filters anomaly events and forwards only high-confidence, eligible ones for SAR validation.

| File | Purpose |
|---|---|
| `main.py` | Entry point. Applies score threshold, geofence, and per-vessel 2-hour cooldown before forwarding. Also triggers the Airflow DAG via CLI. |
| `config.py` | `TriggerBridgeConfig` — score threshold, allowed bounding box, Airflow settings. |
| `filtering.py` | `should_forward_event()` — threshold + geofence logic. `build_trigger_event()` — constructs the `sar.trigger.events` payload. |

**Filtering logic:**
1. Score must be ≥ `SAR_TRIGGER_SCORE_THRESHOLD` (default 0.65)
2. Optional geofence: coordinates must fall within `SAR_TRIGGER_ALLOWED_BBOX`
3. Per-vessel 2-hour cooldown to prevent repeated triggers for the same vessel

**Output topic:** `sar.trigger.events`

---

### 5. SAR Orchestration — `orchestration/`

Apache Airflow manages the satellite validation workflow, triggered via a custom Kafka sensor.

#### DAG: `suspicious_event_validation`

Located at `orchestration/dags/suspicious_event_dag.py`. This is an **event-driven DAG** (`schedule=None`) triggered by the `KafkaTriggerSensor`.

**Task chain:**

```
wait_for_sar_trigger (KafkaTriggerSensor)
  → initialize_incident     (sets STATE_PROCESSING in StateStore)
  → prepare_search_params   (creates 20km buffer BBox, ±24h search window)
  → search_sentinel         (SentinelSearchOperator – queries Copernicus ASF catalog)
  → download_sentinel       (SentinelDownloadOperator – downloads Sentinel-1 imagery)
  → sar_inference           (SARInferenceOperator – preprocesses SAR, runs YOLOv8)
  → finalize_incident       (sets STATE_VERIFIED or STATE_FAILED)
```

#### Custom Operators

| Operator | File | Responsibility |
|---|---|---|
| `KafkaTriggerSensor` | `orchestration/sensors/kafka_trigger_sensor.py` | Polls `sar.trigger.events`, returns payload via XCom |
| `SentinelSearchOperator` | `orchestration/operators/sentinel_search.py` | Queries ASF Copernicus catalog for Sentinel-1 IW/VV scenes |
| `SentinelDownloadOperator` | `orchestration/operators/sentinel_download.py` | Downloads matched SAR products |
| `SARInferenceOperator` | `orchestration/operators/sar_inference.py` | Preprocesses SAR images (TIFF/H5/PNG → normalized PNG), runs oil-spill detection model, parses JSON result from stdout |

The `SARInferenceOperator` supports `.tif/.tiff`, `.h5`, and `.png/.jpg` inputs. BURST images are explicitly rejected (only RTC imagery is supported).

---

## Kafka Topic Contract

| Topic | Direction | Description |
|---|---|---|
| `ais.raw.position_reports` | Ingestion → Stream Processor | Raw, immutable AIS `PositionReport` messages |
| `ais.cleaned.position_reports` | Stream Processor → consumers | Validated, normalised AIS records |
| `ais.features.vessel_tracks` | Stream Processor → Anomaly Detector | Kinematic feature vectors per vessel timestep |
| `ais.anomalies.events` | Anomaly Detector → Trigger Bridge | Scored anomaly events with model metadata |
| `sar.trigger.events` | Trigger Bridge → Airflow / consumers | High-confidence events ready for SAR dispatch |
| `sar.inference.results` | Airflow (planned) → Persistence | SAR detection results (oil_spill / no_spill) |
| `ais.deadletter` | All services | Malformed / unprocessable messages |

All topics carry JSON payloads with a `schema_version` field and ISO-8601 UTC timestamps.

---

## ML Models

### AIS Contrastive Encoder — `preprocessing/`

A self-supervised **Transformer encoder** (`SequenceTransformerEncoder`) trained with **NT-Xent / InfoNCE contrastive loss** to produce voyage-level embeddings.

**Architecture:**
- Input projection: linear → positional encoding
- Transformer encoder layers (configurable depth and width)
- Attention-pooling head (learns which timesteps matter most)
- L2-normalized projection head

**Training (`preprocessing/ais_contrastive_train.py`):**
- Dataset: `VoyageContrastiveDataset` — creates two augmented views of each voyage segment
- Augmentations: random temporal subsampling, time-warping, Gaussian noise, course perturbation
- Loss: InfoNCE with **hard-negative mining** (top-K hardest negatives per anchor)
- W&B integration for experiment tracking
- Outputs: `encoder.pt` checkpoint + `training_history.json`

**Inference (`preprocessing/ais_inference.py`):**
- Builds a multi-level memory bank: **global** (all vessels), **grid-local** (1°×1° cells), **per-vessel stats**, **vessel-type cohort**
- KNN distance scoring via **FAISS** (`IndexFlatL2` or `IndexHNSWFlat`) with NumPy fallback
- Produces per-voyage anomaly scores (Parquet output)

**Memory Bank (`preprocessing/ais_memory_bank.py`):**
Builds the offline reference embeddings used by both the batch inference script and the live `AISRealtimeMemoryBankModel` service.

**Preprocessing (`preprocessing/ais_preprocessing.py`):**
- Loads raw AIS parquets → global cleaning → voyage segmentation → kinematic resampling → physics feature computation → sequence dataset export

---

### Oil Spill Detection Model (SAR) — `training/`

A **YOLOv8 / YOLOv26 object detection model** trained on Sentinel-1 SAR imagery to detect oil slicks.

#### Dataset Construction — `build_dataset.py`

Merges three source classes into a unified YOLO segmentation dataset:
- **Oil spill** samples (positive class): TIFF images + pixel masks
- **No-oil** samples (negative/background): TIFF images + empty masks
- **Lookalike** samples (false-positive candidates): ships, biogenic slicks, etc.

Processing pipeline:
1. Reads multi-channel SAR TIFF files via `tifffile`
2. Per-channel percentile normalization (1st–99th) → 8-bit RGB PNG
3. Contour-based polygon extraction (`cv2.findContours` + `cv2.approxPolyDP`) → YOLO segmentation format
4. Configurable train/val split (default 80/20, seed 42)
5. Outputs `dataset.yaml` and `manifest.csv`

#### Bounding-Box Label Generation — `create_detection_bboxes.py`

Converts segmentation masks into YOLO bounding-box labels for detection training:
1. Loads binary masks (supports single-channel and RGB masks, threshold or exact-value matching)
2. Connected-component analysis (`cv2.connectedComponentsWithStats`) per oil patch
3. Size filtering (min area, min width, min height)
4. **Iterative bbox merging** — merges nearby fragmented boxes within a configurable pixel gap
5. Outputs YOLO format: `<class_id> <cx> <cy> <w> <h>` (normalized)

#### YOLO Detection Training — `training/train_yolo26_bbox.py` / `training/yolo/train_yolo.py`

- **Model:** YOLOv26 (Ultralytics), pretrained, fine-tuned at 1024px resolution
- **Offline augmentation:** Albumentations pipeline (flip, shift-scale-rotate, brightness/contrast, Gaussian noise, motion blur) applied before training
- **Hyperparameter sweeps:** W&B sweep configs in `training/sweeps/`
- W&B experiment tracking with epoch-history and weights artifacts

#### UNet Segmentation Training — `training/unet/`

Alternative pixel-level segmentation model using `segmentation-models-pytorch`.

- `dataset.py` — `OilSpillDataset` with augmentation support
- `train_unet.py` — full training loop with validation metrics
- `sweep_unet.py` — W&B sweep integration

#### SAR Preprocessing — `preprocessing/apply_sar_processing.py`

`preprocess_sar_png()` — applies radar-specific preprocessing (speckle filtering, normalization) on PNG inputs before model inference.

---

## Dataset & Training Pipeline

### Step 1: Build the Segmentation Dataset

```bash
python build_dataset.py \
  --oil-images /path/to/Train_Val_Oil_Spill_images \
  --oil-masks  /path/to/Train_Val_Oil_Spill_masks \
  --no-oil-images /path/to/No_oil \
  --no-oil-masks  /path/to/Mask_no_oil \
  --lookalike-images /path/to/Lookalike \
  --lookalike-masks  /path/to/Mask_lookalike \
  --output datasets/ \
  --val-ratio 0.2 \
  --seed 42
```

### Step 2: Generate Bounding-Box Labels

```bash
python create_detection_bboxes.py \
  --dataset-root datasets/ \
  --min-area 10 \
  --min-width 20 \
  --min-height 20 \
  --merge-gap 12
```

### Step 3: Train the YOLO Detection Model

```bash
python training/train_yolo26_bbox.py \
  --model yolo26x.pt \
  --epochs 100 \
  --imgsz 1024 \
  --batch 16 \
  --aug-per-image 2 \
  --wandb-project oilspill
```

### Step 4: Train the AIS Contrastive Encoder

```bash
python preprocessing/ais_contrastive_train.py \
  --sequences-path preprocessing/outputs/voyage_sequences.npz \
  --output-dir    preprocessing/outputs/ais_model/ \
  --epochs 10 \
  --batch-size 64 \
  --model-dim 128 \
  --emb-dim 64
```

### Step 5: Build Memory Bank

```bash
python preprocessing/ais_memory_bank.py \
  --input-glob "data/ais/*.parquet" \
  --checkpoint preprocessing/outputs/ais_model/encoder.pt \
  --output-dir preprocessing/outputs/ais_memory_bank/
```

---

## Orchestration (Airflow)

The `suspicious_event_validation` DAG runs on Airflow 2.8+ with CeleryExecutor (see `docker-compose.yml`).

**Default credentials:** `airflow` / `airflow`  
**Airflow UI:** [http://localhost:8080](http://localhost:8080)

The DAG can be triggered in two ways:
1. **Automatically** — `KafkaTriggerSensor` polls `sar.trigger.events` every 30 seconds
2. **Manually** — via the Airflow UI with a JSON `dag_run.conf` payload:

```json
{
  "incident_id": "test-001",
  "lat": 53.5,
  "lon": 10.0,
  "timestamp": "2026-05-06T10:00:00Z",
  "score": 0.92
}
```

---

## Project Structure

```text
.
├── ingestion/
│   └── ais_stream/                  # AIS WebSocket & HTTP ingestion
│       ├── run_ingestion.py         # Main WebSocket consumer → Kafka producer
│       ├── run_http_ingestion.py    # Polling REST ingestion
│       ├── run_file_ingestion.py    # Offline file replay
│       ├── ais_ingestion.py         # AISProducerWrapper (Kafka)
│       └── dead_letter/             # DeadLetterHandler
│
├── services/
│   ├── stream_processor/            # AIS cleaning + feature engineering
│   │   ├── main.py
│   │   ├── config.py
│   │   ├── processing.py            # validate_and_normalize, build_feature_event
│   │   └── vessel_state.py          # VesselStateManager (Redis-backed)
│   ├── anomaly_detector/            # Transformer-based anomaly scoring
│   │   ├── main.py
│   │   ├── config.py
│   │   └── model.py                 # AISRealtimeMemoryBankModel
│   ├── trigger_bridge/              # SAR trigger filtering and dispatch
│   │   ├── main.py
│   │   ├── config.py
│   │   └── filtering.py
│   └── ui/                          # (Planned) React monitoring dashboard
│
├── orchestration/
│   ├── dags/
│   │   ├── suspicious_event_dag.py  # Primary event-driven SAR validation DAG
│   │   └── sentinel_polling_dag.py  # Alternative polling DAG
│   ├── operators/
│   │   ├── sentinel_search.py       # SentinelSearchOperator (ASF Copernicus)
│   │   ├── sentinel_download.py     # SentinelDownloadOperator
│   │   └── sar_inference.py         # SARInferenceOperator (YOLOv8)
│   ├── sensors/
│   │   └── kafka_trigger_sensor.py  # KafkaTriggerSensor
│   └── utils/
│       ├── state_store.py           # Incident state cache (PROCESSING/VERIFIED/FAILED)
│       └── geometry.py              # create_buffer_bbox, wkt_from_bbox
│
├── preprocessing/                   # Offline ML training & inference scripts
│   ├── ais_preprocessing.py         # Raw AIS → voyage sequences pipeline
│   ├── ais_contrastive_train.py     # SequenceTransformerEncoder + InfoNCE training
│   ├── ais_inference.py             # Batch hierarchical anomaly scoring
│   ├── ais_memory_bank.py           # Build global/grid/vessel embedding banks
│   ├── ais_visualize_embeddings.py  # UMAP/t-SNE embedding visualization
│   └── apply_sar_processing.py      # SAR-specific image preprocessing
│
├── training/
│   ├── train_yolo26_bbox.py         # YOLO detection training with augmentation
│   ├── yolo/
│   │   ├── train_yolo.py            # YOLO segmentation training
│   │   └── sweeps/                  # W&B sweep configs
│   └── unet/
│       ├── train_unet.py            # UNet segmentation training
│       ├── dataset.py               # OilSpillDataset
│       └── sweep_unet.py            # W&B sweep
│
├── scripts/
│   ├── launch_local.sh              # One-command native local startup
│   ├── stop_local.sh                # Graceful shutdown of all services
│   └── run_inference.py             # CLI inference script for single SAR images
│
├── backend/                         # (Planned) FastAPI REST + WebSocket backend
│   └── app/
│
├── build_dataset.py                 # SAR TIFF → YOLO segmentation dataset builder
├── create_detection_bboxes.py       # Segmentation masks → YOLO bbox labels
├── test_asf.py                      # ASF/Copernicus search connectivity test
├── docker-compose.yml               # Full stack: Kafka, Redis, Postgres, Airflow + microservices
├── Dockerfile                       # Python 3.10-slim image for AIS microservices
├── requirements.txt                 # Python dependencies
├── .env.example                     # Template for all environment variables
├── SYSTEM_ARCHITECTURE_PLAN.md      # Full architectural specification
└── PHASED_IMPLEMENTATION_PLAN.md    # Incremental delivery roadmap
```

---

## Quick Start

### Option A: Docker Compose (Recommended)

Starts the full stack — Kafka (KRaft mode), Redis, PostgreSQL, Airflow (webserver + scheduler + triggerer + init), and all four AIS microservices.

```bash
# 1. Copy and fill in your credentials
cp .env.example .env
# Edit .env: set aisstream_api_key, COPERNICUS_USER, COPERNICUS_PASSWORD, etc.

# 2. Start everything
docker compose up --build

# 3. Open Airflow UI
# http://localhost:8080  (login: airflow / airflow)
```

### Option B: Native Local (Dev / Low Disk)

Requires Kafka and Redis to be running locally (or via the script).

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 2. Install dependencies
pip install -r requirements.txt

# 3. Launch all services
bash scripts/launch_local.sh
```

`launch_local.sh` will:
- Auto-detect and activate your virtualenv/conda environment
- Start Redis and Kafka (Zookeeper + broker) if not already running
- Launch all four microservices as background processes
- Stream logs to `logs/*.log`
- Press **Ctrl+C** to gracefully stop everything

### Service Startup Order (manual)

If launching services individually:

```bash
# Infrastructure first (Kafka + Redis must be up)
python ingestion/ais_stream/run_ingestion.py
python services/stream_processor/main.py
python services/anomaly_detector/main.py
python services/trigger_bridge/main.py
# Then start Airflow scheduler + webserver separately
```

---

## Environment Configuration

Copy `.env.example` to `.env` and fill in your credentials.

### AIS Data Source

| Variable | Default | Description |
|---|---|---|
| `aisstream_api_key` | — | **Required.** API key from [aisstream.io](https://aisstream.io) |
| `AIS_STREAM_BOUNDING_BOXES` | Global | JSON array of `[[min_lat, min_lon], [max_lat, max_lon]]` boxes |

### Kafka

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address(es) |
| `AIS_RAW_TOPIC` | `ais.raw.position_reports` | Raw ingestion topic |
| `AIS_CLEANED_TOPIC` | `ais.cleaned.position_reports` | Cleaned records topic |
| `AIS_FEATURES_TOPIC` | `ais.features.vessel_tracks` | Feature vector topic |
| `AIS_ANOMALIES_TOPIC` | `ais.anomalies.events` | Anomaly events topic |
| `SAR_TRIGGER_TOPIC` | `sar.trigger.events` | SAR trigger topic |
| `AIS_DEADLETTER_TOPIC` | `ais.deadletter` | Dead-letter queue topic |

### Stream Processor

| Variable | Default | Description |
|---|---|---|
| `AIS_STATE_BACKEND` | `redis` | `redis` or `memory` |
| `AIS_REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `AIS_VESSEL_WINDOW_SIZE` | `20` | Sliding window of recent positions per vessel |
| `AIS_VESSEL_STATE_TTL_SEC` | `86400` | Redis key TTL (24 hours) |
| `AIS_VOYAGE_GAP_HOURS` | `2.0` | Gap that resets a vessel's trajectory window |

### Anomaly Detector

| Variable | Default | Description |
|---|---|---|
| `AIS_ANOMALY_SCORE_THRESHOLD` | `0.8` | Minimum combined score to emit an anomaly event |
| `AIS_ENCODER_CHECKPOINT_PATH` | `preprocessing/outputs/ais_model/encoder.pt` | Trained encoder checkpoint |
| `AIS_MEMORY_DIR` | `preprocessing/outputs/ais_memory_bank` | Directory with `.npy` / `.json` memory bank files |
| `AIS_ANOMALY_MODEL_NAME` | `AIS-Contrastive-Encoder-v1` | Model identifier in emitted events |
| `AIS_REALTIME_TRAJECTORY_WINDOW_SIZE` | `30` | Number of timesteps fed to encoder |
| `AIS_REALTIME_K_NEIGHBORS` | `5` | K for KNN memory bank scoring |
| `AIS_REALTIME_USE_FAISS` | `true` | Use FAISS for fast ANN search |

### Trigger Bridge

| Variable | Default | Description |
|---|---|---|
| `SAR_TRIGGER_SCORE_THRESHOLD` | `0.65` | Minimum anomaly score to generate a SAR trigger |
| `SAR_TRIGGER_ALLOWED_BBOX` | (none) | Optional geofence: `min_lat,min_lon,max_lat,max_lon` |

### Sentinel / SAR

| Variable | Default | Description |
|---|---|---|
| `COPERNICUS_USER` | — | ESA Copernicus / ASF username |
| `COPERNICUS_PASSWORD` | — | ESA Copernicus / ASF password |
| `SAR_INFERENCE_CMD` | — | Shell command template for inference; supports `{input}` and `{model}` placeholders |

---

## Infrastructure

| Component | Role |
|---|---|
| **Apache Kafka** (KRaft mode) | Backbone for all async communication between services. No Zookeeper required in Docker setup. |
| **Redis** | Per-vessel trajectory state store (sliding windows, TTL-based expiry). Also used as Airflow Celery broker. |
| **PostgreSQL** | Airflow metadata database. (Also planned as incident/detection store for the UI.) |
| **Apache Airflow 2.8** | CeleryExecutor with webserver, scheduler, and triggerer. Custom sensors/operators for Kafka and Sentinel. |
| **Sentinel-1 (SAR)** | C-band synthetic aperture radar; all-weather, day/night imagery for oil slick detection. IW mode, VV polarization. |
| **FAISS** | Approximate Nearest Neighbour library for fast memory bank lookups in both offline inference and the real-time anomaly detector. |

### Docker Port Map

| Service | Port |
|---|---|
| Airflow UI | `8080` |
| Kafka (host) | `29092` |
| Kafka (container) | `9092` |
| Redis | `6379` |
| PostgreSQL | `5432` |

---

## Development Roadmap

| Phase | Status | Goal |
|---|---|---|
| **Phase 1** — Stream Processor | ✅ Complete | ETL + stateful kinematic features |
| **Phase 2** — Anomaly Detector | ✅ Complete | Contrastive encoder + FAISS memory bank |
| **Phase 3** — SAR Trigger Bridge | ✅ Complete | Score/geofence/cooldown filtering |
| **Phase 4** — Airflow Kafka Sensor | ✅ Complete | Event-driven DAG via `KafkaTriggerSensor` |
| **Phase 5** — PostgreSQL Persistence | 🔜 Planned | `vessels`, `anomalies`, `sar_events`, `detections` tables |
| **Phase 6** — FastAPI + React UI | 🔜 Planned | REST + WebSocket API, live map dashboard |
| **Phase 7** — Hardening + ML Upgrade | 🔜 Planned | Dead-letter replay, Isolation Forest / LSTM models |

---

## Testing

```bash
# Test ASF/Copernicus SAR search connectivity
python test_asf.py

# Test the backend REST API
python backend/test_api.py

# Run inference on a single SAR image
python scripts/run_inference.py --image /path/to/image.png --model runs/yolo/best.pt --task detect
```
