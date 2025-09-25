import os, pandas as pd
from datetime import datetime

def _ensure_dir(path: str): os.makedirs(path, exist_ok=True)

def write_parquet(records: list[dict], root: str, dataset: str, partition_key: str = "ingest_date"):
    if not records: return 0, None
    df = pd.DataFrame.from_records(records)
    if partition_key not in df.columns:
        df[partition_key] = datetime.utcnow().date().isoformat()
    part_vals = df[partition_key].unique()
    for pv in part_vals:
        path = os.path.join(root, dataset, f"{partition_key}={pv}")
        _ensure_dir(path)
        file = os.path.join(path, f"part-{len(os.listdir(path))}.parquet")
        df[df[partition_key]==pv].to_parquet(file, index=False)
    return len(df), part_vals.tolist()
