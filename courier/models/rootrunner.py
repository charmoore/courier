from typing import List

from sqlmodel import SQLModel

from courier.models import Patients, Visits, Locations, Messages


class RootRunner(SQLModel):
    alive: bool = True
    records = []
    records_error = []
    patients: List[Patients] = []
    visits: List[Visits] = []
    locations: List[Locations] = []
    messages: List[Messages] = []
    messages_no_send: List[Messages] = []
    messages_errors: List[Messages] = []
    reports = []
    count_messages: int = 0
    skip_age: int = 0
    skip_expired: int = 0
    skip_out: int = 0
    skip_invalid: int = 0
    skip_old: int = 0
