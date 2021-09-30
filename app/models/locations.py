from sqlmodel import SQLModel, Field


class Locations(SQLModel, table=True):
    LocationID: int = Field(
        default=None, primary_key=True, autoincrement=True, max_length=4
    )
    LocationName: str = Field(max_length=255)
    Address: str = Field(default=None, max_length=255)
    City: str = Field(default=None, max_length=100)
    State: str = Field(default=None, max_length=50)
    Country: str = Field(default=None, max_length=50)
