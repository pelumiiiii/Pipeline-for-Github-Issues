from pydantic import ValidationError
from pydantic import BaseModel
from pipeline.models import GithubIssue

GITHUB_ISSUE_SOURCES = {"github-issues", "github-issues-vscode"}

def validate(source_name: str, rec: dict) -> dict:
    if source_name in GITHUB_ISSUE_SOURCES:
        obj = GithubIssue.model_validate(rec)   # may raise ValidationError
        return obj.model_dump(mode="json")
    return rec
