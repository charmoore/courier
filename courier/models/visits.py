from typing import Optional

from sqlmodel import SQLModel, Field
from datetime import date, datetime


class Visits(SQLModel, table=True):
    VisitID: str = Field(primary_key=True, max_length=50)
    PatientID: int = Field()
    ProviderID: int = Field()
    LocationID: int = Field()
    DateOfService: date
    DatePosted: date
    VisitNumber: Optional[str] = Field(default=None, max_length=255)
    UUID: Optional[str] = Field(default=None, min_length=32, max_length=32)
    Responded: Optional[datetime] = Field(default=None)
    Reported: Optional[date] = Field(default=None)
