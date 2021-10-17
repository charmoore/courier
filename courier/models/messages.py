from typing import Optional

from sqlmodel import SQLModel, Field
from datetime import datetime


class Messages(SQLModel, table=True):
    MessageID: Optional[int] = Field(default=None, primary_key=True)
    TypeID: int
    Updated: datetime
    DTGSent: datetime
    VisitID: str = Field(max_length=50)
    Sent: Optional[int] = Field(default=0)
    ReasonID: int = Field()
    SurveyLink: Optional[str] = Field(default=None, max_length=255)
    Address: Optional[str] = Field(default=None, max_length=255)
    Comments: Optional[str] = Field(default=None, max_length=255)
