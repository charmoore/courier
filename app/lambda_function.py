from lambda_function_dep import lambda_handler
import os
from pathlib import Path
import pandas as pd
import datetime

from config import settings
from utils.logging import Logger
from models.runner import Runner

logger = Logger("base.lambda_function")

class Handler:
    new = []
    history = []
    resend = []
    error = []

    
    def add_new(self,rec):
        self.new.append(rec)

    def add_history(self,rec):
        self.history.append(rec)

    def add_resend(self,rec):
        self.resend.append(rec)
    
    def add_error(self,rec,error):
        self.error.append({"rec":rec,"error":error})

    def __init__(self, event):
        for rec in event['Records']:
            if rec['s3']['object']['key'].startswith('input/'):
                self.add_new(rec)
            elif rec['s3']['object']['key'].startswith('history/'):
                self.add_history(rec)
            elif rec['s3']['object']['key'].startswith('resend/'):
                self.add_resend(rec)
            else:
                self.add_error(rec, "Unknown Bucket")

    def process_new(self):
        # setup vars
        # check if file is valid -> stop if invalid
        # s3 open file
        # read through all lines, append to records list
        # check practice name against record's practice -> error if not
        # iterate through all records, check if they are unique patients, visits, locations
        #? this could be just getting all unique of a column and ensuring they exist
        # if they dont exist, add them to the database

        # check dateservice, whatever that does
        # check the messages in the db to see what the message reason was
        # if there's none, insert one
        pass

    def process_history(self):
        pass

    def process_resend(self):
        pass

    def process_error(self):
        pass

    def process(self):
        self.process_new()
        self.process_history()
        self.process_resend()
        self.process_error()




def setup_log():
    root_path = os.getcwd()
    if settings.log_dir_path:
        log_file_dir = settings.log_dir_path
    else :
        log_file_dir = Path(root_path,"logs")

    logger.start_log(log_path=log_file_dir)

def lambda_handler(event, context):
    setup_log()
    logger.print_and_log(f"Received event(s): {str(event)}")
    handler = Handler(event)

    


if __name__ == '__main__':
    lambda_handler()

