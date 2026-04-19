# End-to-End System Architecture Plan

## AIS -> Anomaly -> SAR -> UI

This plan is tailored to the current repository structure and preserves existing working components while adding missing services in a decoupled, event-driven way.

## 0) Current Baseline (Already in Repo)

Implemented components to keep and build on:

- AIS ingestion producer:
  - `ingestion/ais_stream/run_ingestion.py`
  - `ingestion/ais_stream/ais_ingestion.py`
- Kafka raw and dead-letter publishing:
  - `ais.raw.position_reports`
  - `ais.deadletter`
- Airflow SAR validation DAG and operators:
  - `orchestration/dags/suspicious_event_dag.py`
  - `orchestration/operators/sentinel_search.py`
  - `orchestration/operators/sentinel_download.py`
  - `orchestration/operators/sar_inference.py`
- Incident state utility:
  - `orchestration/utils/state_store.py`

Design invariant:

- `ais.raw.position_reports` remains immutable and append-only.
- No transformation in ingestion layer.

## 1) Target Event-Driven Architecture

```text
AIS WebSocket (aisstream.io)
   -> ais.raw.position_reports
   -> Stream Processor (new service)
   -> ais.cleaned.position_reports
   -> ais.features.vessel_tracks
   -> Anomaly Detector (new service)
   -> ais.anomalies.events
   -> Trigger Filter (new step/service)
   -> sar.trigger.events
   -> Airflow Kafka Sensor (update DAG trigger mode)
   -> Sentinel Search + Download
   -> SAR Inference
   -> sar.inference.results
   -> PostgreSQL/PostGIS
   -> FastAPI + React Dashboard
```

## 2) Kafka Topic Contract

Create/standardize the following topics:

1. `ais.raw.position_reports` (existing)
2. `ais.cleaned.position_reports` (new)
3. `ais.features.vessel_tracks` (new)
4. `ais.anomalies.events` (new)
5. `sar.trigger.events` (new)
6. `sar.inference.results` (new)
7. `ais.deadletter` (existing)

Topic principles:

- JSON payloads with explicit schema version field: `schema_version`.
- Event timestamps in ISO-8601 UTC.
- Use deterministic event IDs for idempotency where possible.

## 3) Service Layout to Add

Add a new top-level `services/` folder:

```text
services/
  stream_processor/
    __init__.py
    main.py
    config.py
    consumer.py
    producer.py
    validators.py
    feature_engineering.py
    vessel_state.py
  anomaly_detector/
    __init__.py
    main.py
    config.py
    rules.py
    detector.py
    trigger_filter.py
  persistence_writer/
    __init__.py
    main.py
    db.py
    writers.py
  api/
    __init__.py
    main.py
    ws.py
    schemas.py
  ui/
    (React app)
```

## 4) Stream Processor (Next Critical Build)

Location: `services/stream_processor/`

Consumes:

- `ais.raw.position_reports`

Responsibilities:

1. Validate and clean records:
   - Ensure `MessageType == PositionReport`
   - Validate coordinate ranges
   - Normalize timestamp format
2. Route malformed records:
   - Publish to `ais.deadletter`
3. Publish normalized records:
   - `ais.cleaned.position_reports`
4. Maintain vessel-level state (Redis recommended):
   - Last N positions
   - Speeds
   - Headings
   - Message time gaps
5. Compute temporal features:
   - Instantaneous speed
   - Acceleration
   - Heading change rate
   - Trajectory window
   - Time gap statistics
6. Publish feature events:
   - `ais.features.vessel_tracks`

State recommendation:

- Redis key per vessel: `vessel:{mmsi}`
- TTL to prevent unbounded growth
- Keep sliding window size configurable (`N=20` default)

## 5) Anomaly Detector Service

Location: `services/anomaly_detector/`

Consumes:

- `ais.features.vessel_tracks`

Phase 1 (AIS inference integrated):

1. Load anomaly scores from `preprocessing/ais_inference.py` output parquet
2. Emit anomalies when model score exceeds threshold
3. Include model metadata in events

Current model name:

- `AIS-Contrastive-Encoder-v1`

Publishes:

- `ais.anomalies.events`

Event schema:

```json
{
  "schema_version": "1.0",
  "event_id": "string",
  "vessel_id": "string",
  "lat": 0.0,
  "lon": 0.0,
  "timestamp": "2026-04-11T10:30:00Z",
   "anomaly_type": "model_detected",
  "score": 0.0,
   "model": {
      "name": "AIS-Contrastive-Encoder-v1",
      "label": "anomalous"
   },
  "features": {}
}
```

Phase 2 (future upgrades):

- Isolation Forest
- LSTM trajectory model
- Transformer sequence model

## 6) SAR Trigger Bridge (Kafka-Only Triggering)

Keep Airflow decoupled from direct API triggers.

Option A (preferred):

- Build trigger filtering in `services/anomaly_detector/trigger_filter.py`

Option B:

- Separate `services/trigger_bridge/`

Consumes:

- `ais.anomalies.events`

Applies filtering:

- `score >= sar_trigger_threshold`
- Optional geofencing/coastal-zone checks

Publishes:

- `sar.trigger.events`

## 7) Airflow Integration Update

Current DAG (`orchestration/dags/suspicious_event_dag.py`) is externally triggered with `dag_run.conf`.

Update strategy:

1. Add a Kafka polling sensor/operator in DAG start.
2. Read one event from `sar.trigger.events`.
3. Map event payload to internal params:
   - `incident_id`
   - `lat`
   - `lon`
   - `timestamp`
4. Keep existing downstream tasks unchanged where possible:
   - `prepare_search_params`
   - `search_sentinel`
   - `download_sentinel`
   - `sar_inference`
   - `finalize_incident`

Result:

- DAG is still event-driven, but now triggered via Kafka backbone, not direct API calls.

## 8) Storage Layer (PostgreSQL + Optional PostGIS)

Add persistent DB for UI queries and auditability.

Recommended tables:

1. `vessels`
   - `vessel_id` PRIMARY KEY
   - `metadata` JSONB
2. `anomalies`
   - `id` PRIMARY KEY
   - `event_id` UNIQUE
   - `vessel_id`
   - `lat`
   - `lon`
   - `timestamp`
   - `anomaly_type`
   - `score`
   - `raw` JSONB
3. `sar_events`
   - `id` PRIMARY KEY
   - `anomaly_id` FK
   - `status` (PROCESSING, VERIFIED, FAILED)
   - `image_path`
   - `raw` JSONB
4. `detections`
   - `id` PRIMARY KEY
   - `sar_event_id` FK
   - `oil_spill_area`
   - `confidence`
   - `mask_path`
   - `raw` JSONB

Writers:

- Consumer for `ais.anomalies.events` -> write `anomalies`
- Consumer for `sar.inference.results` -> write `sar_events` and `detections`

## 9) SAR Inference Result Handling

After `sar_inference` task completes:

1. Publish standardized event(s) to `sar.inference.results`.
2. Persist to DB via persistence writer service.
3. Link inference outputs to originating anomaly/event IDs.

Implementation note:

- Extend finalize phase in `suspicious_event_dag.py` to include Kafka publish step.

## 10) UI Layer

Backend:

- FastAPI service exposing:
  - REST endpoints for historical queries
  - WebSocket endpoint for live events

Frontend:

- React app with map and timeline
- Mapbox or Leaflet for geospatial layers

Minimum viable dashboard:

1. Live vessel positions
2. Anomaly markers with score/type
3. SAR inference overlay references (mask/image links)
4. Event timeline: anomaly -> SAR processing -> detection
5. Basic metrics: anomaly count, verified detections

## 11) Immediate Implementation Priorities

Execute in this order:

1. Build `services/stream_processor` (ETL + state tracking)
2. Build `services/anomaly_detector` (rule-based)
3. Define and create all Kafka topics + JSON schema contracts
4. Add `sar.trigger.events` bridge filter
5. Modify `suspicious_event_dag.py` to consume Kafka trigger events
6. Add PostgreSQL schema + persistence writers
7. Build minimal API + UI monitoring dashboard

## 12) Codebase Fit Notes

To keep integration smooth with current code:

1. Keep existing ingestion files unchanged except config hardening.
2. Reuse current Airflow operators as-is; only adjust DAG entry trigger logic.
3. Keep `orchestration/utils/state_store.py` as incident state cache, but treat PostgreSQL as source of truth for UI.
4. Standardize environment variable names for Sentinel credentials across code and docs (currently mixed conventions appear in repo).
5. Add shared event schema helpers under a common module (e.g., `common/events/`) to avoid schema drift between services.

## 13) Architectural Principles (Non-Negotiable)

1. Fully decoupled services with Kafka as backbone.
2. No direct Airflow API invocation for anomaly-driven execution.
3. Idempotent consumers and deterministic event IDs.
4. Immutable raw topic and auditable state transitions.
5. Persistent storage of critical outputs (not Kafka-only retention).
6. Separation of concerns:
   - Ingestion != Processing != Orchestration != Serving/UI

## 14) Definition of Done (E2E)

System is considered end-to-end ready when:

1. AIS websocket records flow into `ais.raw.position_reports`.
2. Stream processor emits `ais.features.vessel_tracks` for active vessels.
3. Rule-based anomalies are emitted to `ais.anomalies.events`.
4. Trigger filter emits `sar.trigger.events` for high-confidence cases.
5. Airflow consumes trigger events from Kafka and executes SAR DAG.
6. Inference outputs are published to `sar.inference.results`.
7. DB reflects anomalies, SAR events, and detections with relational links.
8. UI displays live anomalies and SAR verification status.
