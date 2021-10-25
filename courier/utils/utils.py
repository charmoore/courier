from typing import Dict
import os
import phonenumbers
from datetime import datetime, timedelta

from phonenumbers.phonenumberutil import NumberParseException

from courier.config import settings
from courier.utils.enums import Reasons

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


def convert_sql_datetime(date_string: str, time: bool = False) -> str:
    if time:
        # change this:
        time = datetime.strftime(
            datetime.strptime(date_string, "%m/%d/%Y"), format=DATE_FORMAT
        )
        return time
    date = datetime.strftime(
        datetime.strptime(date_string, format=DATE_FORMAT), format=DATE_FORMAT
    )
    return date


def valid_phone(raw_number: str, country_code: str = "US") -> tuple[bool, int]:
    # Add AWS flag
    # Try to parse the number and check if its valid
    try:
        phone_number = phonenumbers.parse(raw_number)
    except NumberParseException:
        is_valid = False
    else:
        # You can change to is_possible_number to speed up validity checking to be
        # based on format, rather than checking actual validity
        is_valid = bool(phonenumbers.is_possible_number(phone_number))
    # Check number type
    num_type = phonenumbers.phonenumberutil.number_type(phone_number)
    if num_type in [0]:
        phone_type = 2
    elif num_type in [1, 2, 6]:
        phone_type = 1
    else:
        phone_type = 0

    return is_valid, phone_type

REASONCOMMENTS : Dict[str,str] = {
    "PENDING" : f"Initial entry.  Added {datetime.today()}.",
    "SENT" : 2,
    "UNDER_AGE" : f"A message will not be sent for this visit becuase the patient is under the minimum age of {settings.age_min}.",
    "EXPIRED" : 4,
    "PHONE_BLANK" : 5,
    "EMAIL_BLANK" : 6,
    "PHONE_INVALID" : 7,
    "OPTED_OUT" : 'A message will not be sent for this visit because the patient opted out of messaging.',
    "SEGMENT_PENDING" : 9,
    "ARCHIVE" : "A message will not be sent for this visit, because the date of service is over 30 days.",
    }

def get_reason_message(reason: Reasons) -> str:
    return REASONCOMMENTS.get(reason.name)

