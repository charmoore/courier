from typing import Optional

from sqlmodel import SQLModel, Field


class Providers(SQLModel, table=True):
    ProviderID: Optional[int] = Field(
        primary_key=True,
    )
    Title: Optional[str] = Field(default=None, max_length=8)
    ProviderLast: str = Field(max_length=255)
    ProviderFirst: str = Field(max_length=255)
    ProviderMiddle: Optional[str] = Field(default=None, max_length=255)
    ProviderEmail: Optional[str] = Field(default=None, max_length=255)
    SurveyURL: str = Field(max_length=255)
