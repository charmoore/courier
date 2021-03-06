from pathlib import Path
import logging

from courier.config import settings


class BaseLogger:
    def __init__(
        self,
        log_folder_path: Path,
        log_name="base",
        log_format=None,
        level="INFO",
        log_to_file=True,
    ):
        if not log_folder_path.exists() and log_to_file:
            log_folder_path.mkdir(parents=True)
        if log_to_file:
            logfile_name = f"{log_name}.log"

            self.service_log = logging.getLogger(log_name)
            log_fname = str(log_folder_path / logfile_name)
            logging.basicConfig(
                filename=log_fname,
                format=log_format,
                level=level,
            )

    def set_log_level(self, level="INFO"):
        self.service_log.setLevel(level)

    def log(
        self, message, print_in_console=True, log_level="info", log_to_file=True
    ) -> None:
        if print_in_console:
            print(message)
        if log_to_file:
            logger_function = getattr(self.service_log, log_level.lower())
            if logger_function is None:
                raise ValueError("Log level does not exist")

            logger_function(message)

    def print_and_log(self, message, log_level="info", log_to_file=True) -> None:
        self.log(message, True, log_level, log_to_file)


class Logger:
    def __init__(self, log_name: str = None):
        if log_name is None:
            self.log_name = "base"
        else:
            self.log_name = log_name
        if not settings.log_dir_path:
            self.log_to_file = False

        self.default_log_path = Path(__file__).resolve().parent

        logging.getLogger().level = logging.ERROR

        if settings.LOGGING_DEBUG:
            logging.getLogger("base").level = logging.DEBUG
        else:
            logging.getLogger("base").level = logging.INFO

    def start_log(self, log_path: str = None) -> None:
        if log_path is None:
            lib_log_path = "base"
        else:
            lib_log_path = log_path

        self.logger = BaseLogger(
            lib_log_path / "log",
            log_name="base",
            log_format=settings.LOGGING_FORMAT,
            log_to_file=self.log_to_file,
        )
        self.logger.service_log = logging.getLogger(self.log_name)

    def print_and_log(self, message: str, log_level: str = "info") -> None:
        if not hasattr(self, "logger") and self.log_to_file:
            self.start_log()

        self.logger.print_and_log(message, log_level, self.log_to_file)
