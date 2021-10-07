import os
from datetime import datetime, timedelta

from app.config import settings

DATE_FORMAT = "%Y-%m-%d"


def valid_ext(filepath: str) -> bool:
    return os.path.splitext(os.path.basename(filepath))[1] in settings.formats


def check_date_service(date: datetime, history: bool = False) -> bool:
    if history:
        if settings.practice == "MAG":
            date_started = datetime.strptime(
                date_string="2021-04-03", format=DATE_FORMAT
            )
        elif settings.practice == "OCCPM":
            date_started = datetime.strptime(
                date_string="2021-04-15", format=DATE_FORMAT
            )
        elif settings.practice == "DEMO":
            date_started = datetime.strptime(
                date_string="2021-04-01", format=DATE_FORMAT
            )

        if date < date_started:
            return True
    else:
        if datetime.today() > (date + timedelta(days=30)):
            return True
    return False


def convert_sql_datetime(date_string: str, time: bool = False) -> datetime:
    # Todo: Is this necessary?
    pass
