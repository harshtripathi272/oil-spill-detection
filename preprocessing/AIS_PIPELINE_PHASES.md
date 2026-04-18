# AIS Anomaly Pipeline Phases

This plan implements a global, multi-day AIS behavior pipeline where trajectories are continuous across day boundaries.

## Phase 1: Global AIS Preprocessing and Voyage Construction

Implemented in:

- `preprocessing/ais_preprocessing.py`

Scope:

1. Load all daily parquet files in one unified dataframe.
2. Apply global cleaning:
   - invalid lat/lon
   - impossible speeds
   - malformed timestamps
   - missing critical fields
3. Group by `mmsi` and sort by `timestamp`.
4. Segment into voyages using inter-message gap threshold and minimum trajectory length.
5. Compute physics features.
6. Resample each voyage to fixed interval using kinematic interpolation.
7. Recompute features on resampled trajectory.
8. Build sequence dataset and metadata outputs.

## Phase 2: Global Contrastive Sequence Encoder Training

Implemented in:

- `preprocessing/ais_contrastive_train.py`

Scope:

1. Train a single global encoder (Transformer by default) on all voyage sequences.
2. Use contrastive self-supervised learning with augmentations:
   - subsampling
   - additive noise
   - time warp
   - slight course perturbation
3. Save trained model checkpoint for embedding generation.

## Phase 3: Memory Bank + Vessel Adaptation Stats

Implemented in:

- `preprocessing/ais_memory_bank.py`

Scope:

1. Encode all voyage sequences with trained model.
2. Build global memory bank embeddings.
3. Build optional grid-cell memory subsets based on voyage start location.
4. Build lightweight per-vessel running stats (mean/cov + count).

## Phase 4: Hierarchical Inference Scoring

Implemented in:

- `preprocessing/ais_inference.py`

Scope:

1. Reuse exact preprocessing pipeline for new AIS data.
2. Encode each voyage sequence.
3. Compute combined anomaly score from:
   - physics-rule penalties
   - global/local memory nearest-neighbor distance
   - per-vessel historical deviation after burn-in
4. Flag anomalies using adaptive threshold.

## Suggested Run Order

1. `python -m preprocessing.ais_preprocessing --input-glob "../data/date=*/ais_data.parquet" --output-dir preprocessing/outputs/ais_sequences`
2. `python -m preprocessing.ais_contrastive_train --sequences-path preprocessing/outputs/ais_sequences/voyage_sequences.npz --output-dir preprocessing/outputs/ais_model`
3. `python -m preprocessing.ais_memory_bank --sequences-path preprocessing/outputs/ais_sequences/voyage_sequences.npz --checkpoint preprocessing/outputs/ais_model/encoder.pt --output-dir preprocessing/outputs/ais_memory`
4. `python -m preprocessing.ais_inference --input-glob "../data/date=*/ais_data.parquet" --checkpoint preprocessing/outputs/ais_model/encoder.pt --memory-dir preprocessing/outputs/ais_memory --output-file preprocessing/outputs/ais_inference/anomaly_scores.parquet`
