import os
import datetime
from sqlmodel import SQLModel

from courier.config import settings
from courier.utils.utils import valid_ext
from courier.utils.exceptions import FileExtensionError


class Runner(SQLModel):
    bucket: str
    file_in: str
    file_error: str
    file_errors: str
    file_out: str
    file_processed: str
    file_report: str
    segment_name: str


def build(rec) -> Runner:
    runner = Runner(
        bucket=rec["s3"]["bucket"]["name"],
        file_in=rec["s3"]["object"]["key"],
        file_errors=os.path.splitext(os.path.basename(rec["s3"]["object"]["key"]))[0]
        + "-errors.csv",
        file_error=os.path.splitext(os.path.basename(rec["s3"]["object"]["key"]))[0]
        + "-error.csv",
        file_out=os.path.splitext(os.path.basename(rec["s3"]["object"]["key"]))[0]
        + "-segment.csv",
        file_processed=os.path.splitext(os.path.basename(rec["s3"]["object"]["key"]))[0]
        + "-processed.csv",
        file_report=os.path.splitext(os.path.basename(rec["s3"]["object"]["key"]))[0]
        + "-report.csv",
        segment_name=settings.practice
        + "-"
        + str(datetime.datetime.now().strftime("%Y-%m-%d_%H%M")),
    )
    # Make sure file_in has a valid extension
    if not valid_ext(runner.file_in):
        raise FileExtensionError(os.path.splitext(runner.file_in)[1])

    return runner
