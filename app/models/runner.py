import os
import datetime
from sqlmodel import SQLModel

from app.config import settings
from app.utils.utils import valid_ext
from app.utils.exceptions import FileExtensionError

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
    runner = Runner()
    runner.bucket = rec['s3']['bucket']['name']
    runner.file_in = rec['s3']['object']['key']
    runner.file_errors = os.path.splitext(os.path.basename(runner.file_in))[0] + '-errors.csv'
    runner.file_error = os.path.splitext(os.path.basename(runner.file_in))[0] + '-error.csv'
    runner.file_out = os.path.splitext(os.path.basename(runner.file_in))[0] + '-segment.csv'
    runner.file_processed = os.path.splitext(os.path.basename(runner.file_in))[0] + '-processed.csv'
    runner.file_report = os.path.splitext(os.path.basename(runner.file_in))[0] + '-report.csv'
    runner.segment_name = settings.practice + '-' + str(datetime.datetime.now().strftime('%Y-%m-%d_%H%M'))
    
    # Make sure file_in has a valid extension
    if not valid_ext(runner.file_in):
        raise FileExtensionError(os.path.splitext(runner.file_in)[1])

