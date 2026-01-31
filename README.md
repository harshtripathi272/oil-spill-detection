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
│       └── 🐍 run_ingestion.py
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
