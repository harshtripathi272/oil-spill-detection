# VesselWatch: Oil Spill Detection System

**VesselWatch** is a production-grade monitoring system designed to detect and validate potential oil spills using real-time vessel traffic (AIS) and satellite Synthetic Aperture Radar (SAR) imagery. By fusing terrestrial and maritime data with remote sensing, VesselWatch enables rapid response to environmental threats.

## 🚀 Data Pipeline Architecture

The system operates as a decoupled, event-driven pipeline with five major stages:

### 1. AIS Ingestion Layer
Real-time vessel data is consumed via WebSockets from `aisstream.io`.
- **Primary Source**: `ingestion/ais_stream/run_ingestion.py`
- **Kafka Topic**: `ais.raw.position_reports` (Immutable, append-only)

### 2. Stream Processing Layer
Cleans raw signals and tracks vessel trajectory state.
- **Service**: `services/stream_processor/main.py`
- **Output Topics**: 
  - `ais.cleaned.position_reports`: Normalized AIS records.
  - `ais.features.vessel_tracks`: Computed kinematic features (speed, acceleration, heading change).
- **State Management**: Uses **Redis** to maintain a sliding window of recent positions per vessel.

### 3. Anomaly Detection Layer
Detects suspicious vessel behavior using deep learning and hierarchical scoring.
- **Service**: `services/anomaly_detector/main.py`
- **Model**: `SequenceTransformerEncoder` (`AIS-Contrastive-Encoder-v1`).
- **Scoring Logic**: Combines four dimensions:
  1. **Physics Score**: Detects kinematic impossibilities (e.g., teleportation).
  2. **Global Score**: Comparison against all historical vessel patterns.
  3. **Local Score**: Comparison against patterns specific to the current grid cell ($1^{\circ} \times 1^{\circ}$).
  4. **Vessel Score**: Comparison against the specific vessel's historical behavior.
- **Output Topic**: `ais.anomalies.events`

### 4. Trigger Bridge
Filters and prepares events for satellite validation.
- **Service**: `services/trigger_bridge/main.py`
- **Responsibility**: Selects high-confidence anomalies and maps them to SAR triggers.
- **Output Topic**: `sar.trigger.events`

### 5. SAR Orchestration (Airflow)
Manages the validation workflow using **Apache Airflow**.
- **DAG**: `orchestration/dags/suspicious_event_dag.py`
- **Flow**:
  1. **Kafka Sensor**: `KafkaTriggerSensor` polls `sar.trigger.events`.
  2. **ROI Calculation**: Bounding box created around event coordinates.
  3. **Sentinel Search**: `SentinelSearchOperator` queries the Copernicus catalog.
  4. **Data Retrieval**: `SentinelDownloadOperator` downloads SAR imagery.
  5. **Verification**: `SARInferenceOperator` runs a pretrained **YOLOv8** model to detect oil slicks.
  6. **Finalization**: Updates the **Incident State Store** (`STATE_VERIFIED` or `STATE_FAILED`).

## 📂 Project Structure

```text
.
├── 📁 services              # Real-time microservices
│   ├── 📁 stream_processor  # AIS cleaning and feature engineering
│   ├── 📁 anomaly_detector  # DL-based anomaly detection
│   └── 📁 trigger_bridge    # SAR trigger filtering and mapping
├── 📁 ingestion             # Data ingestion layer
│   └── 📁 ais_stream        # AIS WebSocket consumer and Kafka producer
├── 📁 orchestration          # Airflow workflow management
│   ├── 📁 dags              # Directed Acyclic Graphs (DAGs)
│   ├── 📁 operators         # Custom Airflow operators (Search, Download, Inference)
│   └── 📁 sensors           # Custom Kafka and Sentinel sensors
├── 📁 preprocessing          # Batch training and offline inference scripts
├── 📁 services/ui            # (Planned) React Dashboard
├── ⚙️ docker-compose.yml     # Infrastructure (Kafka, Redis, Airflow)
└── 📄 requirements.txt      # Python dependencies
```

## 🛠️ Infrastructure

-   **Kafka**: The backbone for all asynchronous communication between services.
-   **Redis**: High-speed state store for real-time vessel trajectory windows.
-   **PostgreSQL**: (Planned) Long-term persistence for incidents and detections.
-   **Sentinel-1 (SAR)**: High-resolution radar imagery for all-weather spill detection.

## ⚙️ Environment Configuration

| Variable | Description |
| :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address (default: `localhost:29092`) |
| `REDIS_URL` | Redis connection string (default: `redis://localhost:6379/0`) |
| `COPERNICUS_USER` | Sentinel catalog credentials |
| `SAR_INFERENCE_CMD` | Template command for YOLO inference |
| `AIS_ENCODER_CHECKPOINT` | Path to trained transformer encoder weights |

## 🚀 Running the System

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Launch Services (Order matters)**:
   - Run Ingestion: `python ingestion/ais_stream/run_ingestion.py`
   - Run Stream Processor: `python services/stream_processor/main.py`
   - Run Anomaly Detector: `python services/anomaly_detector/main.py`
   - Run Trigger Bridge: `python services/trigger_bridge/main.py`

3. **Airflow Setup**:
   Ensure `AIRFLOW__CORE__DAGS_FOLDER` points to `orchestration/dags`. The `suspicious_event_validation` DAG will automatically pick up trigger events from Kafka.
