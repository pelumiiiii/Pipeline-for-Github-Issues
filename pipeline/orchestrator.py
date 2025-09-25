import yaml, os
from datetime import datetime, timezone
from pipeline.logging import get_logger
from pipeline.state import get_checkpoint, set_checkpoint
from pipeline.validators.schema import validate, ValidationError
from pipeline.transformers.basic_clean import clean_record
from pipeline.loaders.parquet_local import write_parquet

log = get_logger()

def _load_cfg():
    with open(os.path.join(os.path.dirname(__file__), "config.yaml")) as f:
        return yaml.safe_load(f)

def _import_extractor(kind: str):
    if kind == "http.github": 
        from pipeline.extractors.http_github import extract
    elif kind == "file.csv":
        from pipeline.extractors.csv_local import extract
    else:
        raise ValueError(f"Unknown source kind: {kind}")
    return extract

def run():
    cfg = _load_cfg()
    lake_root = cfg["lake_root"]
    for s in cfg["sources"]:
        name, kind = s["name"], s["kind"]
        log.info(f"=== Source: {name} ({kind}) ===")
        extract = _import_extractor(kind)
        checkpoint_before = get_checkpoint(name)
        rows, bad, good = 0, 0, []
        max_checkpoint = checkpoint_before
        for rec in extract(s.get("options", {}), name):
            rows += 1
            rec = clean_record(rec)

            # CHANGED: validate returns a dict or raises
            try:
                val = validate(name, rec)
            except ValidationError as e:
                bad += 1
                log.warning(f"validation failed: {e}")
                continue

            val["ingest_ts"] = datetime.now(timezone.utc).isoformat()
            
            # track max incremental cursor if configured
            ck = s.get("checkpoint_key")
            if ck and ck in val:
                mc = val[ck]  # ISO string now
                max_checkpoint = mc if not max_checkpoint or mc > max_checkpoint else max_checkpoint

            # Write in micro-batches to avoid huge memory for big pulls
            if len(good) >= 5000:
                n, parts = write_parquet(good, cfg["lake_root"], s["destination"], cfg.get("default_partition","ingest_date"))
                log.info(f"wrote {n} rows to {s['destination']} partitions={parts}")
                good.clear()

        # flush remainder
        if good:
            n, parts = write_parquet(good, cfg["lake_root"], s["destination"], cfg.get("default_partition","ingest_date"))
            log.info(f"wrote {n} rows to {s['destination']} partitions={parts}")

        # update checkpoint *only if* extractor ran successfully
        if s.get("checkpoint_key") and max_checkpoint:
            set_checkpoint(name, max_checkpoint, {"rows_seen": rows, "bad": bad})
        log.info(f"SUMMARY: seen={rows} bad={bad} checkpoint_before={checkpoint_before} checkpoint_after={max_checkpoint}")

if __name__ == "__main__":
    run()
