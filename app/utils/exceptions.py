from app.config import settings

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


class PracticeError(Error):
    def __init__(self, practice):
        self.message = f"Expected {settings.practice} but got {practice}."


class ProviderError(Error):
    def __init__(self, provider):
        self.message = f"Provider name {provider} could not be found in the database."


class FileExtensionError(Error):
    def __init__(self, extension):
        self.message = f"Extension {extension} not in {','.join(settings.formats)}"
