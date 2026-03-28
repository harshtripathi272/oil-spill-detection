# VesselWatch: Oil Spill Detection System

**VesselWatch** is a production-grade monitoring system designed to detect and validate potential oil spills using real-time vessel traffic (AIS) and satellite Synthetic Aperture Radar (SAR) imagery. By fusing terrestrial and maritime data with remote sensing, VesselWatch enables rapid response to environmental threats.

## 🚀 System Flow

1.  **AIS Ingestion**: Real-time vessel data is consumed via WebSockets and streamed into a **Kafka** broker.
2.  **Detection**: Anomaly detection services (or manual triggers) identify suspicious vessel behavior (e.g., unusual stops, speed changes in protected zones).
3.  **Orchestration**: **Apache Airflow** manages the validation pipeline:
    -   **Event Trigger**: A suspicious event initiates the `suspicious_event_validation` DAG.
    -   **ROI Calculation**: The system defines a spatial buffer (Region of Interest) around the event coordinates.
    -   **Satellite Search**: Custom operators query the **Sentinel-1** catalog for SAR products covering the ROI.
    -   **Data Retrieval**: Relevant SAR scenes are downloaded to local/object storage.
4.  **Verification**: 
    -   A pretrained **CNN-based model** runs inference on the SAR imagery to identify oil slicks.
    -   The results are persisted in an **Incident State Store**, marking events as `VERIFIED` or `FALSE_POSITIVE`.

## 📂 Project Structure

```text
.
├── 📁 config                # System and environment configurations
├── 📁 ingestion             # Data ingestion layer
│   └── 📁 ais_stream        # AIS WebSocket consumer and Kafka producer
│       ├── 📁 dead_letter    # Error handling and invalid message storage
│       │   ├── 🐍 invalid_messages.py
│       │   └── ⚙️ see.json
│       ├── 🐍 __init__.py
│       ├── 🐍 ais_ingestion.py
│       ├── 🐍 run_http_ingestion.py
│       ├── 🐍 run_ingestion.py
│       └── 🐍 run_file_ingestion.py
├── 📁 orchestration          # Airflow workflow management
│   ├── 📁 dags              # Directed Acyclic Graphs (DAGs)
│   │   ├── 🐍 sentinel_polling_dag.py     # Periodic satellite search
│   │   └── 🐍 suspicious_event_dag.py      # Event-driven validation
│   ├── 📁 operators         # Custom Airflow operators
│   │   ├── 🐍 sar_inference.py           # ML model inference wrapper
│   │   ├── 🐍 sentinel_download.py      # SAR data downloader
│   │   └── 🐍 sentinel_search.py        # Metadata discovery operator
│   ├── 📁 plugins           # Airflow UI and system plugins
│   ├── 📁 sensors           # Custom polling sensors
│   │   └── 🐍 sentinel_availability_sensor.py # Wait for data availability
│   └── 📁 utils             # Shared libraries
│       ├── 🐍 geometry.py   # Geospatial arithmetic and ROI logic
│       └── 🐍 state_store.py # Incident lifecycle management
├── ⚙️ .gitignore
├── 📝 README.md
├── ⚙️ docker-compose.yml     # Infrastructure (Kafka, Airflow, Zookeeper)
└── 📄 requirements.txt      # Python dependencies
```

## 🛠️ Infrastructure

-   **Kafka**: Real-time message streaming.
-   **Airflow**: Workflow orchestration and task scheduling.
-   **Sentinel-1 (SAR)**: High-resolution radar imagery for all-weather spill detection.
-   **CNN**: Deep learning model for automated pattern recognition in radar data.

## Operational Notes

The SAR validation pipeline is now wired to real Sentinel catalog search/download and command-driven inference.

Required environment variables:

- `COPERNICUS_USER`: Copernicus/ESA username
- `COPERNICUS_PASSWORD`: Copernicus/ESA password
- `SAR_INFERENCE_CMD`: command template used by `SARInferenceOperator`

`SAR_INFERENCE_CMD` placeholders:

- `{input}`: path to downloaded Sentinel product file
- `{model}`: model path from DAG/operator config

Expected `SAR_INFERENCE_CMD` stdout contract (single JSON object per run):

```json
{
    "prediction": "oil_spill",
    "confidence": 0.91,
    "mask_path": "/tmp/masks/example_mask.png"
}
```

Airflow task integration contracts:

- `prepare_search_params` returns `roi_wkt`, `start_date`, `end_date`
- `search_sentinel` returns list of product objects, each including `product_id`
- `download_sentinel` returns list of local downloaded file paths
- `sar_inference` returns list of inference result objects

Example DAG trigger payload (`suspicious_event_validation`):

```json
{
    "incident_id": "inc-1234",
    "lat": 45.25,
    "lon": 10.55,
    "timestamp": "2026-03-28T09:00:00Z"
}
```

## Additional AIS Data Source (File Replay)

In addition to live WebSocket ingestion, this project now supports replaying AIS data from files into Kafka.

Runner:

- `ingestion/ais_stream/run_file_ingestion.py`

Supported input formats:

- NDJSON (one JSON object per line)
- JSON array (`[{...}, {...}]`)
- Single JSON object

Usage:

```bash
python ingestion/ais_stream/run_file_ingestion.py --input ingestion/ais_stream/dead_letter/see.json
```

Optional limit for controlled replay:

```bash
python ingestion/ais_stream/run_file_ingestion.py --input path/to/messages.ndjson --max-messages 1000
```

Behavior:

- Valid `PositionReport` messages are published to `ais.raw.position_reports`
- Invalid/non-PositionReport records are routed to `ais.deadletter`

## Additional AIS Data Source (HTTP Polling)

For a real alternate live source (besides WebSocket), use HTTP polling against an AIS provider API.

Runner:

- `ingestion/ais_stream/run_http_ingestion.py`

Required env vars:

- `AIS_HTTP_URL`

Optional env vars:

- `AIS_HTTP_API_KEY`
- `AIS_HTTP_AUTH_HEADER` (default: `Authorization`)
- `AIS_HTTP_AUTH_SCHEME` (default: `Bearer`)
- `AIS_HTTP_POLL_INTERVAL_SEC` (default: `30`)
- `AIS_HTTP_TIMEOUT_SEC` (default: `20`)
- `AIS_HTTP_RETRY_ATTEMPTS` (default: `3`)
- `AIS_HTTP_RETRY_BACKOFF_SEC` (default: `2`)

Run command:

```bash
python ingestion/ais_stream/run_http_ingestion.py --url https://your-ais-provider.example.com/v1/positions
```

Notes:

- Supports provider payloads as list or dictionary wrappers (`messages`, `data`, `results`, `items`, `records`).
- Accepts both AIS-stream-like `PositionReport` objects and flat records (`mmsi`, `lat`, `lon`, `timestamp`) with normalization.
- Publishes valid normalized records to `ais.raw.position_reports` and sends invalid records to `ais.deadletter`.
