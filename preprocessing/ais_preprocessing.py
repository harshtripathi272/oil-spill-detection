from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

# Feature order used by training/inference.
SEQUENCE_FEATURE_COLUMNS = [
    "sog",
    "acceleration",
    "turn_rate",
    "heading_cog_diff",
    "norm_speed",
    "time_of_day_sin",
    "time_of_day_cos",
    "cog_sin",
    "cog_cos",
    "distance_km",
]


@dataclass
class AISPreprocessConfig:
    max_sog_knots: float = 80.0
    voyage_gap_hours: float = 2.0
    min_voyage_points: int = 8
    min_voyage_minutes: float = 30.0
    resample_interval: str = "10min"


def load_ais_parquets(input_glob: str) -> pd.DataFrame:
    paths = sorted(Path(p) for p in Path().glob(input_glob))
    if not paths:
        raise FileNotFoundError(f"No AIS parquet files matched: {input_glob}")

    frames: List[pd.DataFrame] = []
    for path in paths:
        df = pd.read_parquet(path)
        df["source_file"] = str(path)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined


def clean_ais_globally(df: pd.DataFrame, cfg: AISPreprocessConfig) -> pd.DataFrame:
    required = ["mmsi", "timestamp", "latitude", "longitude", "sog", "cog", "heading", "length"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    out = out.dropna(subset=["mmsi", "timestamp", "latitude", "longitude", "sog"])

    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out["sog"] = pd.to_numeric(out["sog"], errors="coerce")
    out["cog"] = pd.to_numeric(out["cog"], errors="coerce")
    out["heading"] = pd.to_numeric(out["heading"], errors="coerce")
    out["length"] = pd.to_numeric(out["length"], errors="coerce")

    out = out.dropna(subset=["latitude", "longitude", "sog", "length"])
    out = out[(out["latitude"].between(-90, 90)) & (out["longitude"].between(-180, 180))]
    out = out[(out["sog"] >= 0) & (out["sog"] <= cfg.max_sog_knots)]

    out["cog"] = np.mod(out["cog"], 360.0)
    out["heading"] = np.mod(out["heading"], 360.0)

    out = out.sort_values(["mmsi", "timestamp"]).drop_duplicates(["mmsi", "timestamp"], keep="last")
    return out.reset_index(drop=True)


def segment_voyages(df: pd.DataFrame, cfg: AISPreprocessConfig) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    for mmsi, grp in df.groupby("mmsi", sort=False):
        g = grp.sort_values("timestamp").copy()
        gap_hours = g["timestamp"].diff().dt.total_seconds().div(3600.0)
        split = (gap_hours > cfg.voyage_gap_hours) | gap_hours.isna()
        g["voyage_idx"] = split.cumsum() - 1

        for voyage_idx, voyage in g.groupby("voyage_idx", sort=False):
            duration_min = (voyage["timestamp"].iloc[-1] - voyage["timestamp"].iloc[0]).total_seconds() / 60.0
            if len(voyage) < cfg.min_voyage_points:
                continue
            if duration_min < cfg.min_voyage_minutes:
                continue

            voyage = voyage.copy()
            start = voyage["timestamp"].iloc[0].strftime("%Y%m%dT%H%M%S")
            voyage["voyage_id"] = f"{mmsi}_{start}_{int(voyage_idx)}"
            parts.append(voyage)

    if not parts:
        raise ValueError("No voyages produced after segmentation filters")

    return pd.concat(parts, ignore_index=True)


def _haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    r = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(np.clip(1 - a, 0, 1)))
    return r * c


def _angle_diff_deg(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return (b - a + 180.0) % 360.0 - 180.0


def compute_physics_features(df: pd.DataFrame) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    for voyage_id, grp in df.groupby("voyage_id", sort=False):
        g = grp.sort_values("timestamp").copy()

        dt_sec = g["timestamp"].diff().dt.total_seconds()
        dt_sec = dt_sec.fillna(0.0).clip(lower=0.0)

        prev_lat = g["latitude"].shift(1).to_numpy()
        prev_lon = g["longitude"].shift(1).to_numpy()
        curr_lat = g["latitude"].to_numpy()
        curr_lon = g["longitude"].to_numpy()

        distance_km = np.zeros(len(g), dtype=np.float64)
        valid = ~np.isnan(prev_lat) & ~np.isnan(prev_lon)
        distance_km[valid] = _haversine_km(prev_lat[valid], prev_lon[valid], curr_lat[valid], curr_lon[valid])

        sog = g["sog"].to_numpy(dtype=np.float64)
        accel = np.zeros(len(g), dtype=np.float64)
        ds = np.diff(sog, prepend=sog[0])
        safe_dt = np.where(dt_sec.to_numpy() > 0, dt_sec.to_numpy(), np.nan)
        accel = np.divide(ds, safe_dt, out=np.zeros_like(ds), where=~np.isnan(safe_dt))

        cog = g["cog"].to_numpy(dtype=np.float64)
        heading = g["heading"].to_numpy(dtype=np.float64)

        d_cog = _angle_diff_deg(np.roll(cog, 1), cog)
        d_cog[0] = 0.0
        turn_rate = np.divide(d_cog, safe_dt, out=np.zeros_like(d_cog), where=~np.isnan(safe_dt))

        heading_cog_diff = _angle_diff_deg(heading, cog)

        length = np.maximum(g["length"].to_numpy(dtype=np.float64), 1.0)
        norm_speed = sog / length

        hour = (
            g["timestamp"].dt.hour.to_numpy()
            + g["timestamp"].dt.minute.to_numpy() / 60.0
            + g["timestamp"].dt.second.to_numpy() / 3600.0
        )
        phase = 2.0 * np.pi * hour / 24.0
        time_of_day_sin = np.sin(phase)
        time_of_day_cos = np.cos(phase)

        cog_rad = np.radians(cog)
        cog_sin = np.sin(cog_rad)
        cog_cos = np.cos(cog_rad)

        g["dt_sec"] = dt_sec
        g["distance_km"] = distance_km
        g["acceleration"] = accel
        g["turn_rate"] = turn_rate
        g["heading_cog_diff"] = heading_cog_diff
        g["norm_speed"] = norm_speed
        g["time_of_day_sin"] = time_of_day_sin
        g["time_of_day_cos"] = time_of_day_cos
        g["cog_sin"] = cog_sin
        g["cog_cos"] = cog_cos

        parts.append(g)

    return pd.concat(parts, ignore_index=True)


def _kinematic_step(lat: float, lon: float, sog_knots: float, cog_deg: float, dt_sec: float) -> Tuple[float, float]:
    if dt_sec <= 0 or not np.isfinite(dt_sec):
        return lat, lon

    speed_mps = max(float(sog_knots), 0.0) * 0.514444
    distance_m = speed_mps * dt_sec
    brg = np.radians(float(cog_deg))

    d_north = distance_m * np.cos(brg)
    d_east = distance_m * np.sin(brg)

    d_lat = d_north / 111320.0
    cos_lat = max(np.cos(np.radians(lat)), 1e-6)
    d_lon = d_east / (111320.0 * cos_lat)

    return lat + d_lat, lon + d_lon


def resample_voyages_kinematic(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    for voyage_id, grp in df.groupby("voyage_id", sort=False):
        g = grp.sort_values("timestamp").copy()
        g = g.drop_duplicates("timestamp", keep="last")

        ts_index = pd.date_range(start=g["timestamp"].iloc[0], end=g["timestamp"].iloc[-1], freq=interval, tz="UTC")
        if len(ts_index) < 2:
            continue

        base = g.set_index("timestamp").reindex(ts_index)
        base.index.name = "timestamp"
        base["observed"] = base["mmsi"].notna()

        # Fill static/categorical columns.
        base["mmsi"] = base["mmsi"].ffill().bfill()
        for col in ["vessel_name", "imo", "call_sign", "vessel_type", "status", "cargo", "transceiver", "source_file"]:
            if col in base.columns:
                base[col] = base[col].ffill().bfill()
        for col in ["length", "width", "draft"]:
            if col in base.columns:
                base[col] = base[col].interpolate(method="time").ffill().bfill()

        # Interpolate kinematics and angular features in circular space.
        base["sog"] = base["sog"].interpolate(method="time").ffill().bfill()

        for angle_col in ["cog", "heading"]:
            sin_col = f"_{angle_col}_sin"
            cos_col = f"_{angle_col}_cos"
            a = np.radians(base[angle_col])
            base[sin_col] = np.sin(a)
            base[cos_col] = np.cos(a)
            base[sin_col] = base[sin_col].interpolate(method="time").ffill().bfill()
            base[cos_col] = base[cos_col].interpolate(method="time").ffill().bfill()
            base[angle_col] = (np.degrees(np.arctan2(base[sin_col], base[cos_col])) + 360.0) % 360.0
            base = base.drop(columns=[sin_col, cos_col])

        # Kinematic interpolation for lat/lon, anchored by observed points.
        lat = base["latitude"].copy()
        lon = base["longitude"].copy()

        first_idx = lat.first_valid_index()
        if first_idx is None:
            continue

        lat_prev = float(lat.loc[first_idx])
        lon_prev = float(lon.loc[first_idx])

        prev_t = first_idx
        for t in base.index:
            if pd.notna(lat.loc[t]) and pd.notna(lon.loc[t]):
                lat_prev = float(lat.loc[t])
                lon_prev = float(lon.loc[t])
                prev_t = t
                continue

            dt_sec = (t - prev_t).total_seconds()
            lat_prev, lon_prev = _kinematic_step(
                lat=lat_prev,
                lon=lon_prev,
                sog_knots=float(base.loc[t, "sog"]),
                cog_deg=float(base.loc[t, "cog"]),
                dt_sec=dt_sec,
            )
            lat.loc[t] = lat_prev
            lon.loc[t] = lon_prev
            prev_t = t

        base["latitude"] = lat
        base["longitude"] = lon
        base["voyage_id"] = voyage_id

        frames.append(base.reset_index())

    if not frames:
        raise ValueError("No voyages were resampled")

    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"index": "timestamp"})
    return out


def build_sequence_dataset(df: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    sequence_list: List[np.ndarray] = []
    metadata_rows: List[Dict[str, object]] = []

    for voyage_id, grp in df.groupby("voyage_id", sort=False):
        g = grp.sort_values("timestamp").copy()

        X = g[SEQUENCE_FEATURE_COLUMNS].copy()
        X = X.replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0.0)
        arr = X.to_numpy(dtype=np.float32)
        if arr.shape[0] < 2:
            continue

        sequence_list.append(arr)

        metadata_rows.append(
            {
                "voyage_id": voyage_id,
                "mmsi": str(g["mmsi"].iloc[0]),
                "start_timestamp": g["timestamp"].iloc[0],
                "end_timestamp": g["timestamp"].iloc[-1],
                "duration_sec": (g["timestamp"].iloc[-1] - g["timestamp"].iloc[0]).total_seconds(),
                "start_lat": float(g["latitude"].iloc[0]),
                "start_lon": float(g["longitude"].iloc[0]),
                "n_steps": int(arr.shape[0]),
            }
        )

    if not sequence_list:
        raise ValueError("No sequences were produced")

    np.savez_compressed(
        output_dir / "voyage_sequences.npz",
        sequences=np.array(sequence_list, dtype=object),
        feature_columns=np.array(SEQUENCE_FEATURE_COLUMNS, dtype=object),
    )

    metadata = pd.DataFrame(metadata_rows)
    metadata.to_parquet(output_dir / "voyage_metadata.parquet", index=False)


def run_preprocessing(input_glob: str, output_dir: Path, cfg: AISPreprocessConfig) -> None:
    raw = load_ais_parquets(input_glob=input_glob)
    cleaned = clean_ais_globally(raw, cfg=cfg)
    voyages = segment_voyages(cleaned, cfg=cfg)

    features_raw = compute_physics_features(voyages)
    features_raw.to_parquet(output_dir / "voyages_features_raw.parquet", index=False)

    resampled = resample_voyages_kinematic(voyages, interval=cfg.resample_interval)
    features_resampled = compute_physics_features(resampled)
    features_resampled.to_parquet(output_dir / "voyages_features_resampled.parquet", index=False)

    build_sequence_dataset(features_resampled, output_dir=output_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Global AIS preprocessing and voyage sequence construction")
    parser.add_argument(
        "--input-glob",
        type=str,
        default="../data/date=*/ais_data.parquet",
        help="Glob pattern for daily AIS parquet files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("preprocessing/outputs/ais_sequences"),
        help="Output directory for processed features and sequences.",
    )
    parser.add_argument("--max-sog", type=float, default=80.0)
    parser.add_argument("--voyage-gap-hours", type=float, default=2.0)
    parser.add_argument("--min-voyage-points", type=int, default=8)
    parser.add_argument("--min-voyage-minutes", type=float, default=30.0)
    parser.add_argument("--resample-interval", type=str, default="10min")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    cfg = AISPreprocessConfig(
        max_sog_knots=args.max_sog,
        voyage_gap_hours=args.voyage_gap_hours,
        min_voyage_points=args.min_voyage_points,
        min_voyage_minutes=args.min_voyage_minutes,
        resample_interval=args.resample_interval,
    )

    run_preprocessing(input_glob=args.input_glob, output_dir=args.output_dir, cfg=cfg)
    print(f"Preprocessing complete. Outputs saved to: {args.output_dir}")


if __name__ == "__main__":
    main()
