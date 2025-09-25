"""Builds the Silver layer for GitHub issues with engineered features and metadata."""

from __future__ import annotations

import json
import hashlib
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow.dataset as ds

from pipeline.logging import get_logger

FEATURE_COLUMNS = [
    "title_length",
    "title_word_count",
    "issue_age_days",
    "time_since_update_days",
    "is_recent_update",
    "is_weekend_created",
    "repo_issue_count_30d",
    "repo_issue_count_90d",
    "user_issue_count_30d",
    "user_issue_count_90d",
    "title_has_bug",
    "title_has_error",
]
LABEL_COLUMN = "priority_label"
log = get_logger(__name__)


def _hash_config(cfg: dict | None, config_path: Path | None) -> str:
    payload = {
        "config": cfg or {},
        "config_path": str(config_path) if config_path else None,
    }
    raw = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def _load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    dataset = ds.dataset(str(path), format="parquet")
    table = dataset.to_table()
    if table.num_rows == 0:
        return pd.DataFrame()
    return table.to_pandas()


def _rolling_count_window(df: pd.DataFrame, group_col: str, window_days: int) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")
    window = pd.Timedelta(days=window_days)
    result = pd.Series(0.0, index=df.index)
    for _, group in df.groupby(group_col):
        group = group.sort_values("created_at")
        group = group.dropna(subset=["created_at"])
        if group.empty:
            continue
        times = group["created_at"].tolist()
        idx = group.index.tolist()
        counts = pd.Series(0.0, index=idx, dtype="float64")
        start = 0
        for end, current_time in enumerate(times):
            while start < end and current_time - times[start] > window:
                start += 1
            counts.iloc[end] = float(end - start)
        result.loc[idx] = counts
    return result


def _get_commit_hash(repo_root: Path) -> str | None:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root)
            .decode()
            .strip()
        )
    except Exception:
        return None


def build_github_silver(
    sources: Iterable[dict],
    lake_root: str,
    pipeline_config: dict | None = None,
    config_path: Path | None = None,
) -> dict | None:
    """Create the Silver layer datasets and metadata for GitHub issue sources."""
    lake_path = Path(lake_root)
    bronze_frames: list[pd.DataFrame] = []

    required_cols = {"repo_owner", "repo_name", "created_at", "updated_at", "ingest_ts", "title", "state", "comments", "id", "user_login"}

    for source in sources:
        dest = source.get("destination")
        if not dest:
            continue
        bronze_path = lake_path / dest
        df = _load_parquet(bronze_path)
        if df.empty:
            continue
        if not required_cols.issubset(df.columns):
            # Skip legacy files missing the new schema additions
            continue
        bronze_frames.append(df)

    if not bronze_frames:
        return None

    all_records = pd.concat(bronze_frames, ignore_index=True)
    raw_row_count = len(all_records)
    for col in ["created_at", "updated_at", "closed_at", "ingest_ts"]:
        if col in all_records.columns:
            all_records[col] = pd.to_datetime(all_records[col], utc=True, errors="coerce")

    all_records["repo_full_name"] = all_records["repo_owner"].astype(str) + "/" + all_records["repo_name"].astype(str)

    # Rolling aggregates computed on the full history
    all_records = all_records.sort_values("created_at")
    all_records["repo_issue_count_30d"] = _rolling_count_window(all_records, "repo_full_name", 30)
    all_records["repo_issue_count_90d"] = _rolling_count_window(all_records, "repo_full_name", 90)
    all_records["user_issue_count_30d"] = _rolling_count_window(all_records, "user_login", 30)
    all_records["user_issue_count_90d"] = _rolling_count_window(all_records, "user_login", 90)

    # Deduplicate per issue by latest update/ingest timestamp
    latest = (
        all_records.sort_values(["updated_at", "ingest_ts"], ascending=True)
        .drop_duplicates(subset="id", keep="last")
    )
    dedup_row_count = len(latest)

    # Only score open issues
    latest = latest[latest["state"].str.lower() == "open"].copy()
    if latest.empty:
        return None

    reference_raw = latest["ingest_ts"].max()
    if pd.isna(reference_raw):
        reference_time = pd.Timestamp(datetime.now(timezone.utc))
    else:
        reference_time = pd.Timestamp(reference_raw)
    if reference_time.tzinfo is None:
        reference_time = reference_time.tz_localize("UTC")
    else:
        reference_time = reference_time.tz_convert("UTC")

    latest["title"] = latest["title"].fillna("")
    latest["title_length"] = latest["title"].str.len()
    latest["title_word_count"] = latest["title"].str.split().apply(len)
    created_ts = latest["created_at"].dt.tz_convert("UTC")
    updated_ts = latest["updated_at"].dt.tz_convert("UTC")
    latest["issue_age_days"] = (reference_time - created_ts) / pd.Timedelta(days=1)
    latest["time_since_update_days"] = (reference_time - updated_ts) / pd.Timedelta(days=1)
    latest["is_recent_update"] = (latest["time_since_update_days"] <= 7).astype(int)
    latest["is_weekend_created"] = latest["created_at"].dt.dayofweek.isin([5, 6]).astype(int)
    latest["title_has_bug"] = latest["title"].str.contains(r"\bbug\b", case=False).astype(int)
    latest["title_has_error"] = latest["title"].str.contains(r"error", case=False).astype(int)

    latest[LABEL_COLUMN] = (latest["comments"].fillna(0) >= 5).astype(int)

    # Ensure we don't leak the raw comment count into features
    feature_columns = [col for col in FEATURE_COLUMNS if col in latest.columns]
    for col in feature_columns:
        latest[col] = latest[col].fillna(0)

    base_columns = [
        "id",
        "repo_owner",
        "repo_name",
        "number",
        "state",
        "user_login",
        "created_at",
        "updated_at",
        "ingest_ts",
    ]
    keep_columns = [col for col in base_columns if col in latest.columns] + [LABEL_COLUMN] + feature_columns
    latest = latest[keep_columns]

    latest = latest.sort_values("ingest_ts")
    n = len(latest)
    train_end = max(int(n * 0.7), 1)
    val_end = max(int(n * 0.85), train_end + 1)

    splits = {
        "train": latest.iloc[:train_end],
        "val": latest.iloc[train_end:val_end],
        "test": latest.iloc[val_end:],
    }

    silver_root = lake_path / "silver" / "github" / "issues"
    silver_root.mkdir(parents=True, exist_ok=True)

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_dir = silver_root / f"run_ts={run_stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    latest_dir = silver_root / "latest"
    if latest_dir.exists():
        for child in latest_dir.glob("*"):
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    for split, frame in splits.items():
        if frame.empty:
            continue
        split_dir_run = run_dir / f"split={split}"
        split_dir_run.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(split_dir_run / "data.parquet", index=False)

        split_dir_latest = latest_dir / f"split={split}"
        split_dir_latest.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(split_dir_latest / "data.parquet", index=False)

    missing_pct = {col: float(latest[col].isna().mean()) for col in latest.columns}
    commit_hash = _get_commit_hash(lake_path.parent)
    config_hash = _hash_config(pipeline_config or {}, config_path)

    data_quality = {
        "rows_raw": raw_row_count,
        "rows_after_dedupe": dedup_row_count,
        "rows_current_open": n,
        "duplicates_removed": raw_row_count - dedup_row_count,
        "split_rows": {split: int(len(frame)) for split, frame in splits.items()},
        "missing_pct": missing_pct,
    }

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_cutoff": reference_time.isoformat() if reference_time is not None else None,
        "total_rows": n,
        "feature_columns": feature_columns,
        "label_column": LABEL_COLUMN,
        "commit": commit_hash,
        "config_hash": config_hash,
        "source_destinations": [source.get("destination") for source in sources],
        "quality": data_quality,
        "run_directory": str(run_dir.relative_to(lake_path)),
        "feature_version": "v1.1",
    }

    run_meta_path = run_dir / "_meta.json"
    run_meta_path.write_text(json.dumps(meta, indent=2, default=str))

    latest_meta_path = latest_dir / "_meta.json"
    latest_meta_path.write_text(json.dumps(meta, indent=2, default=str))

    log.info(
        "silver snapshot created run=%s rows=%s duplicates_removed=%s",
        run_stamp,
        n,
        data_quality["duplicates_removed"],
    )

    return meta


__all__ = ["build_github_silver", "FEATURE_COLUMNS", "LABEL_COLUMN"]
