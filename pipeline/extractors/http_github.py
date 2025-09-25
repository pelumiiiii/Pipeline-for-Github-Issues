import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from pipeline.logging import get_logger
from pipeline.state import get_checkpoint


BASE = "https://api.github.com"
log = get_logger(__name__)

def _headers():
    # Optional: set GITHUB_TOKEN in env to avoid low rate limits
    
    tok = os.getenv("GITHUB_TOKEN")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

#Retrieve Github issues
def _request_with_retry(url: str, *, params: dict, headers: dict, timeout: float, max_attempts: int, backoff_seconds: float):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            log.warning(
                "request error (%s/%s) for %s: %s; retrying in %.1fs",
                attempt,
                max_attempts,
                url,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)
            continue

        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            reset_at = resp.headers.get("X-RateLimit-Reset")
            if reset_at:
                wait_seconds = max(float(reset_at) - datetime.now(timezone.utc).timestamp(), 0)
            else:
                wait_seconds = backoff_seconds * (2 ** (attempt - 1))
            if attempt == max_attempts:
                raise RuntimeError("GitHub rate limit exceeded and retries exhausted")
            log.warning("GitHub rate limit hit; sleeping %.1fs before retry", wait_seconds)
            time.sleep(max(wait_seconds, 1))
            continue

        if resp.status_code >= 500 and attempt < max_attempts:
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            log.warning(
                "server error (%s) for %s; retrying in %.1fs",
                resp.status_code,
                url,
                sleep_for,
            )
            time.sleep(sleep_for)
            continue

        return resp

    raise RuntimeError("unreachable")


def extract(options: dict, source_name: str):
    owner, repo = options["owner"], options["repo"]
    per_page = options.get("per_page", 100)
    since = get_checkpoint(source_name)  # ISO-8601 string or None
    timeout = options.get("request_timeout", 30)
    max_attempts = options.get("max_attempts", 3)
    backoff_seconds = options.get("backoff_seconds", 1.5)

    #loop through 100 issues per page
    page = 1

    #samples for viewing only, not needed for actual run
    samples_root = options.get("samples_dir")
    if samples_root:
        samples_dir = Path(samples_root)
    else:
        samples_dir = Path(__file__).resolve().parent.parent.parent / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    while True:
        params = {"state":"all","per_page": per_page, "page": page}
        if since: params["since"] = since 
        r = _request_with_retry(
            f"{BASE}/repos/{owner}/{repo}/issues",
            params=params,
            headers=_headers(),
            timeout=timeout,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
        if r.status_code == 422:
            log.info("GitHub returned 422 for %s/%s page=%s; stopping pagination at API limit", owner, repo, page)
            break
        r.raise_for_status()#raise error for bad responses
        batch = r.json()

        #Just to view samples, not needed for actual run
        if not batch: break
        sample_path = samples_dir / f"github_issues_page_{page}.json"
        sample_path.write_text(json.dumps(batch, indent=2))


        # Flatten nested keys we care about (not pull requests,PR*)
        for it in batch:
            if "pull_request" in it:   # skip PRs if you want pure issues
                continue
            out = {
              "id": it["id"], "number": it["number"], "title": it["title"],
              "state": it["state"], "user.login": it["user"]["login"],
              "comments": it["comments"], "created_at": it["created_at"],
              "updated_at": it["updated_at"], "closed_at": it.get("closed_at"),
              "repo_owner": owner,
              "repo_name": repo,
            }
            yield out
        page += 1
