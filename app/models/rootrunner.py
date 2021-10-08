from typing import List

from sqlmodel import SQLModel


class RootRunner(SQLModel):
    alive: bool = True
    records = []
    records_error = []
    patients = []
    visits = []
    locations = []
    messages = []
    messages_no_send = []
    messages_errors = []
    reports = []
    count_messages: int = 0
    skip_age: int = 0
    skip_expired: int = 0
    skip_out: int = 0
    skip_invalid: int = 0
    skip_old: int = 0
