# Data Contracts

## GitHub Issues (bronze/github/issues*)
| Field | Type | Description |
| --- | --- | --- |
| `id` | integer | Immutable GitHub issue identifier. |
| `number` | integer | Repository-scoped issue number. |
| `title` | string | Issue title, cleaned of leading/trailing whitespace. |
| `state` | string | Issue state (`open`, `closed`, etc.). |
| `user_login` | string | GitHub login of the author (alias of `user.login`). |
| `comments` | integer | Count of comments at extraction time. |
| `created_at` | timestamp | ISO-8601 creation time (UTC). |
| `updated_at` | timestamp | ISO-8601 last update time (UTC). Used as checkpoint cursor. |
| `closed_at` | timestamp/null | Closure time when applicable. |
| `ingest_ts` | timestamp | Pipeline ingestion timestamp (UTC). |

### Quality Rules
- All records must pass Pydantic validation before landing in bronze storage.
- `updated_at` increases monotonically per source run and drives checkpointing.
- Records with `pull_request` keys are skipped to avoid mixing PRs with issues.

## Local Sales CSV (bronze/sales/raw)
- Schema is inferred from CSV headers. Keep CSV files consistent and document any changes via pull requests.

### Change Management
- Breaking schema changes require updating this contract and migrating downstream tables.
- Notify data consumers before modifying required fields or data types.
