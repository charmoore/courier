from typing import Optional
from sqlmodel import SQLModel, Field


class Locations(SQLModel, table=True):
    LocationID: Optional[int] = Field(
        default=None,
        primary_key=True,
    )
    LocationName: Optional[str] = Field(max_length=255)
    Address: Optional[str] = Field(default=None, max_length=255)
    City: Optional[str] = Field(default=None, max_length=100)
    State: Optional[str] = Field(default=None, max_length=50)
    Country: Optional[str] = Field(default=None, max_length=50)
