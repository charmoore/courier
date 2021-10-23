from enum import IntEnum, Enum


class Reasons(IntEnum):
    PENDING = 1
    SENT = 2
    UNDER_AGE = 3
    EXPIRED = 4
    PHONE_BLANK = 5
    EMAIL_BLANK = 6
    PHONE_INVALID = 7
    OPTED_OUT = 8
    SEGMENT_PENDING = 9
    ARCHIVE = 10


class MessageTypes(IntEnum):
    NULL = 0
    PENDING = 1
    INITIAL_SMS = 2
    INITIAL_EMAIL = 3
    FOLLOWUP_SMS = 4
    FOLLOWUP_EMAIL = 5
    RESENT_SMS = 6
    RESENT_EMAIL = 7


class ReasonComments(Enum):
    PENDING = 1
    SENT = 2
    UNDER_AGE = 3
    EXPIRED = 4
    PHONE_BLANK = 5
    EMAIL_BLANK = 6
    PHONE_INVALID = 7
    OPTED_OUT = 8
    SEGMENT_PENDING = 9
    ARCHIVE = "A message will not be sent for this visit, because the date of service is over 30 days."

    def __init__(self, reason: Reasons):
        self.reason = reason.name

    def get_message(self):
        self.message = self[self.reason].value
        return self.message


if __name__ == "__main__":
    comment = ReasonComments(Reasons.ARCHIVE)
    print(comment)
