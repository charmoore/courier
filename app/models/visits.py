from typing import Optional

from sqlmodel import SQLModel, Field
from uuid import UUID
from datetime import date, datetime


class Visits(SQLModel, table=True):
    VisitID: str = Field(primary_key=True, max_length=50)
    PatientID: int = Field(max_length=10)
    ProviderID: int = Field(max_length=10)
    LocationID: int = Field(max_length=4)
    DateOfService: date
    DatePosted: date
    VisitNumber: Optional[str] = Field(default=None, max_length=255)
    UUID: Optional[UUID] = Field(default=None, max_length=32)
    Responded: Optional[datetime] = Field(default=None)
    Reported: Optional[date] = Field(default=None)
