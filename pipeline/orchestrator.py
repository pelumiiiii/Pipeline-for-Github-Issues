import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pipeline.logging import get_logger
from pipeline.state import get_checkpoint, set_checkpoint
from pipeline.validators.schema import validate, ValidationError
from pipeline.transformers.basic_clean import clean_record
from pipeline.loaders.parquet_local import write_parquet
from pipeline.silver.github_issues import build_github_silver

log = get_logger()

def _resolve_config_path() -> Path:
    env_path = os.getenv("PIPELINE_CONFIG_PATH")
    if env_path:
        cfg_path = Path(env_path)
        if not cfg_path.exists():
            raise FileNotFoundError(f"PIPELINE_CONFIG_PATH={env_path} does not exist")
        return cfg_path

    base_path = Path(__file__).resolve().with_name("config.yaml")
    env_name = os.getenv("PIPELINE_ENV")
    if env_name:
        candidate = base_path.with_name(f"config.{env_name}.yaml")
        if candidate.exists():
            return candidate
    return base_path


def _load_cfg():
    cfg_path = _resolve_config_path()
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    log.info(f"loaded configuration from {cfg_path}")
    return cfg, cfg_path

def _import_extractor(kind: str):
    if kind == "http.github": 
        from pipeline.extractors.http_github import extract
    elif kind == "file.csv":
        from pipeline.extractors.csv_local import extract
    else:
        raise ValueError(f"Unknown source kind: {kind}")
    return extract

def run():
    cfg, cfg_path = _load_cfg()
    lake_root = cfg["lake_root"]
    github_sources = [s for s in cfg["sources"] if s["kind"] == "http.github"]
    for s in cfg["sources"]:
        name, kind = s["name"], s["kind"]
        log.info(f"=== Source: {name} ({kind}) ===")
        extract = _import_extractor(kind)
        checkpoint_before = get_checkpoint(name)
        rows, bad, good = 0, 0, []
        max_checkpoint = checkpoint_before
        source_failed = False
        try:
            for rec in extract(s.get("options", {}), name):
                rows += 1
                rec = clean_record(rec)

                try:
                    val = validate(name, rec)
                except ValidationError as e:
                    bad += 1
                    log.warning(f"validation failed: {e}")
                    continue

                val["ingest_ts"] = datetime.now(timezone.utc).isoformat()

                ck = s.get("checkpoint_key")
                if ck and ck in val:
                    mc = val[ck]
                    max_checkpoint = mc if not max_checkpoint or mc > max_checkpoint else max_checkpoint

                good.append(val)

                if len(good) >= 5000:
                    n, parts = write_parquet(
                        good,
                        lake_root,
                        s["destination"],
                        cfg.get("default_partition", "ingest_date"),
                    )
                    log.info(f"wrote {n} rows to {s['destination']} partitions={parts}")
                    good.clear()
        except Exception as exc:
            source_failed = True
            log.error(
                "source %s encountered an error after %s rows: %s",
                name,
                rows,
                exc,
                exc_info=True,
            )

        if good and not source_failed:
            n, parts = write_parquet(
                good,
                lake_root,
                s["destination"],
                cfg.get("default_partition", "ingest_date"),
            )
            log.info(f"wrote {n} rows to {s['destination']} partitions={parts}")
        elif good and source_failed:
            log.warning(f"discarding {len(good)} buffered rows for {name} due to earlier failure")

        if not source_failed and s.get("checkpoint_key") and max_checkpoint:
            set_checkpoint(name, max_checkpoint, {"rows_seen": rows, "bad": bad})
        status = "failed" if source_failed else "completed"
        log.info(
            f"SUMMARY[{status.upper()}]: seen={rows} bad={bad} checkpoint_before={checkpoint_before} checkpoint_after={max_checkpoint}"
        )

    if github_sources:
        try:
            meta = build_github_silver(github_sources, lake_root, cfg, cfg_path)
            if meta:
                log.info(
                    "github silver build completed rows=%s train=%s val=%s test=%s duplicates_removed=%s",
                    meta.get("total_rows"),
                    meta.get("split_rows", {}).get("train", 0),
                    meta.get("split_rows", {}).get("val", 0),
                    meta.get("split_rows", {}).get("test", 0),
                    meta.get("duplicates_removed"),
                )
        except Exception as exc:
            log.error("failed to build github silver layer: %s", exc, exc_info=True)

if __name__ == "__main__":
    run()
