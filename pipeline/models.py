from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class GithubIssue(BaseModel):
    id: int
    number: int
    title: str
    state: str
    user_login: str = Field(alias="user.login")
    comments: int
    created_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime] = None

