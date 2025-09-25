# Top-level plumbing helpers for validating basic transformations and retry logic
from collections import deque

import pytest

import pandas as pd

from pipeline.transformers.basic_clean import clean_record
from pipeline.validators.schema import validate


def test_clean_record_strips_strings_and_nulls_empty():
    raw = {"title": "  Hello  ", "body": " ", "count": 7}
    cleaned = clean_record(raw)
    assert cleaned["title"] == "Hello"
    assert cleaned["body"] is None
    assert cleaned["count"] == 7


def test_validate_github_issue_applies_alias():
    record = {
        "id": 42,
        "number": 999,
        "title": "Example",
        "state": "open",
        "user.login": "octocat",
        "comments": 0,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T01:00:00Z",
        "closed_at": None,
        "repo_owner": "org",
        "repo_name": "repo",
    }
    validated = validate("github-issues", record)
    assert validated["user_login"] == "octocat"
    assert validated["updated_at"].startswith("2025-01-01T01:00:00")

# Minimal response double for the extractor retry tests
class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error should have been handled")


# Exercise pagination and PR-filtering behaviour with canned pages
@pytest.mark.parametrize("status_code,pages,expected", [
    (422, [None], []),
    (200, [[
        {
            "id": 1,
            "number": 1,
            "title": "Issue",
            "state": "open",
            "user": {"login": "octocat"},
            "comments": 0,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T01:00:00Z",
        },
        {
            "id": 2,
            "number": 2,
            "title": "PR",
            "state": "open",
            "user": {"login": "bot"},
            "comments": 0,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T01:00:00Z",
            "pull_request": {},
        },
    ], []], [1]),
])
def test_http_github_extract(monkeypatch, tmp_path, status_code, pages, expected):
    from pipeline.extractors import http_github

    monkeypatch.setattr(http_github, "get_checkpoint", lambda _: None)

    responses = deque()
    if status_code == 422:
        responses.append(DummyResponse(422, []))
    else:
        for payload in pages:
            responses.append(DummyResponse(200, payload))

    def fake_get(*args, **kwargs):
        if responses:
            return responses.popleft()
        return DummyResponse(200, [])

    monkeypatch.setattr(http_github.requests, "get", fake_get)

    records = list(
        http_github.extract(
            {
                "owner": "org",
                "repo": "repo",
                "per_page": 100,
                "samples_dir": str(tmp_path / "samples"),
            },
            "github-issues",
        )
    )

    assert [r["id"] for r in records] == expected
    assert all("pull_request" not in r for r in records)


def test_request_with_retry_handles_backoff(monkeypatch, tmp_path):
    from pipeline.extractors import http_github
    from requests import RequestException

    rate_limited = DummyResponse(403, [])
    rate_limited.headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "0"}

    sequence = deque([
        RequestException("timeout"),
        rate_limited,
        DummyResponse(200, []),
    ])

    def fake_get(*args, **kwargs):
        value = sequence.popleft()
        if isinstance(value, Exception):
            raise value
        return value

    sleeps = []

    monkeypatch.setattr(http_github.requests, "get", fake_get)
    monkeypatch.setattr(http_github.time, "sleep", lambda secs: sleeps.append(secs))
    monkeypatch.setattr(http_github, "get_checkpoint", lambda _: None)

    list(
        http_github.extract(
            {
                "owner": "org",
                "repo": "repo",
                "per_page": 1,
                "max_attempts": 5,
                "backoff_seconds": 0.1,
                "samples_dir": str(tmp_path / "samples"),
            },
            "github-issues",
        )
    )

    assert sleeps  # ensure retry logic triggered an actual pause


def test_build_github_silver_creates_splits(tmp_path, monkeypatch):
    from pipeline.silver.github_issues import build_github_silver

    bronze_dir = tmp_path / "bronze/github/issues"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    base_time = pd.Timestamp("2025-01-01T00:00:00Z")
    data = pd.DataFrame(
        [
            {
                "id": 1,
                "number": 10,
                "title": "Fix bug in API",
                "state": "open",
                "user_login": "alice",
                "comments": 6,
                "created_at": base_time,
                "updated_at": base_time + pd.Timedelta(days=1),
                "closed_at": pd.NaT,
                "ingest_ts": base_time + pd.Timedelta(days=2),
                "repo_owner": "org",
                "repo_name": "repo",
            },
            {
                "id": 1,
                "number": 10,
                "title": "Fix bug in API",
                "state": "open",
                "user_login": "alice",
                "comments": 7,
                "created_at": base_time,
                "updated_at": base_time + pd.Timedelta(days=3),
                "closed_at": pd.NaT,
                "ingest_ts": base_time + pd.Timedelta(days=4),
                "repo_owner": "org",
                "repo_name": "repo",
            },
            {
                "id": 2,
                "number": 11,
                "title": "Add docs",
                "state": "open",
                "user_login": "bob",
                "comments": 1,
                "created_at": base_time + pd.Timedelta(days=5),
                "updated_at": base_time + pd.Timedelta(days=6),
                "closed_at": pd.NaT,
                "ingest_ts": base_time + pd.Timedelta(days=7),
                "repo_owner": "org",
                "repo_name": "repo",
            },
        ]
    )
    data.to_parquet(bronze_dir / "part.parquet", index=False)

    sources = [
        {
            "name": "github-issues",
            "kind": "http.github",
            "destination": "bronze/github/issues",
        }
    ]

    cfg = {
        "lake_root": str(tmp_path),
        "sources": sources,
    }

    meta = build_github_silver(sources, str(tmp_path), cfg, tmp_path / "config.yaml")
    assert meta is not None
    silver_root = tmp_path / "silver/github/issues"
    run_dirs = list(silver_root.glob("run_ts=*"))
    assert run_dirs
    latest_dir = silver_root / "latest"
    assert (latest_dir / "split=train/data.parquet").exists()
    assert (latest_dir / "_meta.json").exists()
    assert meta["label_column"] == "priority_label"
