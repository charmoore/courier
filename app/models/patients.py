from typing import Optional

from sqlmodel import SQLModel, Field
from datetime import date, datetime


class Patients(SQLModel, table=True):
    PatientID: Optional[int] = Field(primary_key=True, default=None)
    Imported: datetime
    Updated: datetime
    PatientLast: str = Field(max_length=255)
    PatientFirst: str = Field(max_length=255)
    PatientMiddle: Optional[str] = Field(default=None, max_length=255)
    Age: int = Field(max_digits=3)
    Death: Optional[date] = Field(default=None)
    Phone: Optional[str] = Field(default=None, max_length=20)
    PhoneType: Optional[int] = Field(default=None, max_digits=2)
    Email: Optional[str] = Field(default=None, max_length=100)
    OptOut: Optional[int] = Field(default=0, max_digits=1)
