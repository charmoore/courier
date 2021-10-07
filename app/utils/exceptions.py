# Boto3 client connection exception


# Generic message exception
class Error(Exception):
    """
    Base class for exceptions in this module
    """

    pass


class GenericError(Error):
    """Generic Error message"""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"There was an error: {self.message}"


class StateError(Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"[FAILED] Check the error folder or the logs."


class DBConnectionError(Error):
    def __init__(self, message):
        self.message = message
