import requests, itertools
import os, json
from pathlib import Path
from pipeline.state import get_checkpoint
from datetime import datetime, timezone
from pipeline.logging import get_logger


BASE = "https://api.github.com"
log = get_logger(__name__)

def _headers():
    # Optional: set GITHUB_TOKEN in env to avoid low rate limits
    
    tok = os.getenv("GITHUB_TOKEN")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

#Retrieve Github issues
def extract(options: dict, source_name: str):
    owner, repo = options["owner"], options["repo"]
    per_page = options.get("per_page", 100)
    since = get_checkpoint(source_name)  # ISO-8601 string or None

    #loop through 100 issues per page
    page = 1

    #samples for viewing only, not needed for actual run
    samples_dir = Path(__file__).resolve().parent.parent.parent / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    while True:
        params = {"state":"all","per_page": per_page, "page": page}
        if since: params["since"] = since 
        r = requests.get(f"{BASE}/repos/{owner}/{repo}/issues", params=params, headers=_headers(), timeout=30)
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
              "updated_at": it["updated_at"], "closed_at": it.get("closed_at")
            }
            yield out
        page += 1
