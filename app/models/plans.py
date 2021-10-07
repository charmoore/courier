from typing import Optional

from sqlmodel import SQLModel, Field


class Plans(SQLModel, table=True):
    PlanID: int = Field(primary_key=True, default=None)
    PlanName: str = Field(max_length=50)
    Description: Optional[str] = Field(default=None, max_length=255)
