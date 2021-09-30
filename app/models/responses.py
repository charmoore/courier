from typing import Optional

from sqlmodel import SQLModel, Field
from datetime import datetime


class Responses(SQLModel, table=True):
    ID: Optional[int] = Field(default=None, primary_key=True, autoincrement=True)
    VisitID: str = Field(max_length=50)
    SurveyID: str = Field(max_length=255)
    DTG: datetime
    QuestionID: int = Field(max_length=255)
    Question: str = Field(max_length=255)
    AnswerID: int = Field(max_length=255)
    Answer: str = Field(max_length=255)
    Score: Optional[int] = Field(default=None, max_length=2)
