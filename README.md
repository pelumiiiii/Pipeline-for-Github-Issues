# Pipeline(ML*)

Robust yet lightweight data-ingestion pipeline that pulls operational data from GitHub and local CSV sources, validates each record, and writes columnar outputs to a lake-style filesystem layout. The project is built in Python to showcase incremental ingestion, schema enforcement, and modular extract/transform/load (ETL) patterns.

## Features
- **Incremental HTTP extraction** from GitHub issues with checkpoint persistence and graceful pagination limits.
- **Schema-aware validation** via Pydantic models to guarantee downstream data quality.
- **Config-driven orchestration** so multiple sources can be added without code changes.
- **Local lakehouse output** using partitioned Parquet files for analytics-friendly storage.
- **Sample capture utilities** that snapshot API responses for inspection and debugging.

## Project Layout
```
Pipeline(ML*)/
├── pipeline/                # Core application modules
│   ├── orchestrator.py      # Main entrypoint that runs configured sources
│   ├── config.yaml          # Source definitions and destinations
│   ├── extractors/          # Source-specific connectors (GitHub, CSV)
│   ├── transformers/        # Record normalization helpers
│   ├── validators/          # Pydantic schemas and validation logic
│   ├── loaders/             # Parquet writer for the local lake
│   └── logging.py           # Shared logging configuration
├── samples/                 # Captured payloads and demo CSV inputs
├── lake/                    # Output data lake (bronze layer)
├── pipeline_state.db        # SQLite backing store for incremental checkpoints
├── requirements.txt         # Python dependencies
└── tests/                   # Unit and integration tests (add yours here)
```

## Getting Started
1. **Create or activate a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **(Optional) Configure credentials**
   - Set `GITHUB_TOKEN` in your environment to unlock higher API rate limits.
   - Override `PIPELINE_STATE_DB` if you want checkpoint state stored elsewhere.
4. **Review `pipeline/config.yaml`**
   - Each item under `sources:` represents an independent ingestion job.
   - Adjust owners/repos or add new sections to ingest additional datasets.
5. **Run the orchestrator**
   ```bash
   python -m pipeline.orchestrator
   ```
   Logs stream to stdout. Summaries include counts of processed records and checkpoint details.

## Adding New Sources
1. Implement an extractor in `pipeline/extractors/` that yields dictionaries.
2. (Optional) Define a Pydantic model under `pipeline/models.py` to validate the new payload.
3. Update `pipeline/validators/schema.py` so your source name invokes the appropriate model.
4. Register the extractor in `pipeline/orchestrator.py::_import_extractor` if it uses a new `kind`.
5. Add the configuration block to `pipeline/config.yaml` with a unique `name` and `destination`.

## Samples & Lake Output
- Raw API snapshots are written to `samples/github_issues_page_*.json` for debugging or demos.
- Normalized records land under `lake/bronze/<namespace>` as partitioned Parquet files (default partition key `ingest_date`).

## Development & Testing
- Run `pytest` (once tests are added) from the project root to validate transformations.
- Keep linting/formatting consistent; tools such as `ruff` or `black` can be added via pre-commit hooks.
- When updating schemas or config, ensure checkpoints (`pipeline_state.db`) are reset if you need a clean replay.

## Operational Notes
- The GitHub API exposes only the first 1,000 results per query; the extractor handles 422 responses gracefully and stops at the limit.
- Checkpoints are stored per-source; delete the relevant row from `pipeline_state.db` to reprocess from scratch.
- For production, consider adding retry/backoff logic, observability metrics, and CI pipelines before deployment.

## Contributing
1. Fork or branch from `main`.
2. Make your changes and add tests where practical.
3. Run the pipeline and ensure outputs remain consistent.
4. Submit a pull request describing the change, expected impact, and validation steps.

## License
Clarify licensing before distribution. Add a LICENSE file if the project is shared outside your organization.
