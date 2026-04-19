# Phased Implementation Plan

This roadmap translates the architecture into incremental, testable phases that fit the current repository.

## Phase 1: Stream Processor (ETL + Stateful Features)

Goal:

- Convert raw AIS events into cleaned records and vessel trajectory features.

Scope:

1. Consume `ais.raw.position_reports`
2. Validate/normalize PositionReport payloads
3. Route malformed events to `ais.deadletter`
4. Publish cleaned events to `ais.cleaned.position_reports`
5. Maintain per-vessel rolling state (Redis-backed with in-memory fallback)
6. Compute temporal features and publish to `ais.features.vessel_tracks`

Deliverables:

- `services/stream_processor` runnable service
- Configurable via environment variables
- Structured logs and graceful shutdown

Exit criteria:

- Consuming from raw topic works continuously
- Invalid records reach DLQ
- Cleaned + feature topics receive valid events

## Phase 2: Model-Based Anomaly Detector

Goal:

- Detect suspicious vessel behavior from feature stream.

Scope:

1. Consume `ais.features.vessel_tracks`
2. Maintain a sliding voyage window per MMSI
3. Encode the live window with the trained AIS Transformer encoder
4. Score the embedding against the preloaded memory bank with FAISS
5. Publish `ais.anomalies.events` with score and anomaly metadata
6. Add deterministic event IDs for idempotency

Current model:

- `AIS-Contrastive-Encoder-v1`

Exit criteria:

- Feature stream produces scored anomaly events above configured threshold

## Phase 3: SAR Trigger Bridge

Goal:

- Convert anomaly events into SAR trigger events using Kafka-only decoupling.

Scope:

1. Consume `ais.anomalies.events`
2. Apply score threshold + optional geofencing
3. Publish to `sar.trigger.events`

Exit criteria:

- Only eligible anomalies become SAR trigger events

## Phase 4: Airflow Kafka Sensor Integration

Goal:

- Trigger existing SAR workflow from Kafka events instead of direct API invocation.

Scope:

1. Update `orchestration/dags/suspicious_event_dag.py` entry logic to pull from `sar.trigger.events`
2. Map event fields to current DAG task contracts
3. Keep existing operator chain unchanged where possible

Exit criteria:

- New trigger event starts DAG and runs Sentinel + inference flow end-to-end

## Phase 5: Persistence Layer (PostgreSQL)

Goal:

- Persist anomaly and SAR lifecycle for querying and audit.

Scope:

1. Add DB schema (`vessels`, `anomalies`, `sar_events`, `detections`)
2. Add Kafka persistence consumers for `ais.anomalies.events` and `sar.inference.results`
3. Add idempotent upsert logic by event IDs

Exit criteria:

- DB reflects complete anomaly->SAR->detection chain

## Phase 6: API + Real-Time UI

Goal:

- Expose live and historical monitoring UX.

Scope:

1. FastAPI backend (REST + WebSocket)
2. React dashboard with map, anomalies, SAR overlays, timeline
3. Basic metrics cards

Exit criteria:

- Operators can observe vessel activity, alerts, and SAR outcomes in real time

## Phase 7: Hardening and ML Upgrade

Goal:

- Improve reliability and anomaly quality.

Scope:

1. Dead-letter replay and reprocessing strategy
2. Consumer idempotency and backfill support
3. Optional model-based anomaly detection (Isolation Forest/LSTM/Transformer)
4. Monitoring and alerting

Exit criteria:

- Stable operation under load and recoverable failure paths
