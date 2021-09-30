from sqlmodel import SQLModel, Field


class Plans(SQLModel, table=True):
    PlanID: int = Field(primary_key=True, default=None, max_length=2)
    PlanName: str = Field(max_length=50)
    Description: str = Field(defualt=None, max_length=255)
