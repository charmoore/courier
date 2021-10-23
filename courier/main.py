import os
from pathlib import Path
import pandas as pd
import time
from fastapi import Depends
import cProfile
import pstats
import traceback
import datetime

from courier.config import settings
from courier.db import get_db
from courier.utils.logging import Logger
from courier.utils.enums import Reasons, MessageTypes
from courier.models.runner import build as Runner
from courier.crud import crud
from courier.models import Patients, Visits, Locations, Messages
from courier.models.rootrunner import RootRunner
from courier.utils.utils import convert_sql_datetime, valid_phone, check_date_service
from courier.utils.exceptions import (
    StateError,
    GenericError,
    FileExtensionError,
    PracticeError,
    ProviderError,
)

logger = Logger("base.lambda_function")

LINE = "-------------------"


class Handler:
    new = []
    history = []
    resend = []
    error = []

    def add_new(self, rec):
        self.new.append(rec)

    def add_history(self, rec):
        self.history.append(rec)

    def add_resend(self, rec):
        self.resend.append(rec)

    def add_error(self, rec, error):
        self.error.append({"rec": rec, "error": error})

    def __init__(self, event):
        try:
            for rec in event["Records"]:
                if rec["s3"]["object"]["key"].startswith("input/"):
                    self.add_new(rec)
                elif rec["s3"]["object"]["key"].startswith("history/"):
                    self.add_history(rec)
                elif rec["s3"]["object"]["key"].startswith("resend/"):
                    self.add_resend(rec)
                else:
                    self.add_error(rec, "Unknown Bucket")
        except Exception as e:
            logger.print_and_log(traceback.print_exc())

    def process_new(self):
        # initialize runner instance
        root_runner = RootRunner()
        for rec in self.new:
            try:
                runner = Runner(rec)
                logger.print_and_log(runner)
                with settings.s3.open(runner.bucket, runner.file_in, "r") as f:
                    df = pd.read_csv(f)
                for index, row in df.iterrows():
                    if settings.practice not in row["PracticeName"]:
                        raise PracticeError(practice=row["PracticeName"])
                    root_runner.records.append(row)
                    if (
                        not row["SurveyRequestID"]
                        or not row["PatientID"]
                        or not row["PatientName"]
                        or not row["ServicingProvider"]
                        or not row["DateOfService"]
                        or not row["PostDate"]
                        or not row["LocationName"]
                    ):
                        raise GenericError(
                            f"A critical field was blank, skipping row {index}"
                        )
                    with get_db() as db:
                        if not crud.patients.exists(db=db, id=row["PatientID"]):
                            name_arr = row["PatientName"].splint(",")
                            patientLast = name_arr[0]
                            _, patientFirst, patientMiddle = name_arr[1].split(" ")
                            (phone, phoneType) = valid_phone(row["Phone"])
                            patient = Patients(
                                PatientID=row["PatientID"],
                                PatientFirst=patientFirst,
                                PatientLast=patientLast,
                                PatientMiddle=patientMiddle,
                                Imported=datetime.datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                Age=int(row["Age"]),
                                Email=row["Email"],
                                Phone=phone,
                                PhoneType=phoneType,
                                Death=convert_sql_datetime(row["DateOfDeath"])
                                if row["DateOfDeath"]
                                else None,
                            )
                            root_runner.patients.append(patient)
                        if not crud.visits.exists(db=db, id=row["SurveyRequestID"]):
                            date_of_service = (
                                convert_sql_datetime(row["DateOfService"])
                                if row["DateOfService"]
                                else None
                            )
                            # Check date of service
                            if check_date_service(row["DateOfService"]):
                                message = Messages(Reas)
                                root_runner.messages_no_send.append()
                            post_date = (
                                convert_sql_datetime(row["PostDate"])
                                if row["PostDate"]
                                else None
                            )
                            location_id = (
                                row["LocationName"]
                                .partition(" ")[0]
                                .replace("(", "")
                                .replace(")", "")
                            )
                            provider = crud.providers.get_by_names(
                                db=db, name=row["ServicingProvider"]
                            )
                            if not provider:
                                raise ProviderError(row["ServicingProvider"])
                            visit = Visits(
                                VisitID=row["SurveyRequestID"],
                                PatientID=row["PatientID"],
                                LocationID=location_id,
                                ProviderID=provider.ProviderID,
                                DateOFService=date_of_service,
                                DatePosted=post_date,
                                VisitNumber=row["VisitNumber"],
                            )
                            root_runner.visits.append(visit)  # append as list of models
                        if not crud.locations.exists(db=db, id=row["LocationName"]):
                            if "(" in row["LocationName"]:
                                locationID = (
                                    row["LocationName"]
                                    .partition(" ")[0]
                                    .replace("(", "")
                                    .replace(")", "")
                                )
                                location = Locations(
                                    LocationID=locationID,
                                )
                            else:
                                location = Locations(LocationName=row["LocationName"])
                            root_runner.locations.append(
                                location
                            )  # append as list of models
                # Now that they're sorted, insert all of them
                if root_runner.patients:
                    logger.print_and_log(
                        f"Inserting {len(root_runner.patients)} patients"
                    )
                    temp = crud.patients.create_many(db=db, objs=root_runner.patients)
                    logger.print_and_log(f"{len(temp)} patients inserted.")
                if root_runner.locations:
                    logger.print_and_log(
                        f"Inserting {len(root_runner.locations)} locations"
                    )
                    temp = crud.locations.create_many(db=db, objs=root_runner.locations)
                    logger.print_and_log(f"{len(temp)} locations inserted.")
                if root_runner.visits:
                    logger.print_and_log(f"Inserting {len(root_runner.visits)} visits")
                    temp = crud.visits.create_many(db=db, objs=root_runner.visits)
                    logger.print_and_log(f"{len(temp)} visits inserted.")
                # ! next: line 654

            except FileExtensionError as e:
                # add to error dict
                self.add_error(rec=rec, error=e)
                file = f"s3://{runner.bucket}/{runner.file_in}"
                destination = f"s3://{runner.bucket}/error/{runner.file_error}"
                logger.print_and_log(
                    f"Due to error, moving object {file} to {destination}"
                )
                settings.s3.move(object=file, destination=destination)
            except PracticeError as e:
                self.add_error(rec=rec, error=e)
                file = f"{runner.bucket}/{runner.file_in}"
                destination = f"{runner.bucket}/error/{runner.file_error}"
                logger.print_and_log(
                    f"Due to error, moving object {file} to {destination}"
                )
                settings.s3.move(object=file, destination=destination)
            except GenericError as e:
                self.add_error(rec=rec, error=e)

        # * a lot of the db work can genuinely be wrapped in a try-catch-finally

        # setup vars
        # check if file is valid -> stop if invalid
        # s3 open file
        # read through all lines, append to records list
        # check practice name against record's practice -> error if not
        # iterate through all records, check if they are unique patients, visits, locations
        # ? this could be just getting all unique of a column and ensuring they exist
        # if they dont exist, add them to the database

        # check dateservice, whatever that does
        # check the messages in the db to see what the message reason was (type 0, reason 10)
        # if there's none, add to messages_no_send
        # else, add to error list with ("exists")

        # check the patient isnt old enough
        # check if a message exists on the patient with type 0, reason 3,,
        # if theres none, add to messages_no_send, else add to errors

        # check if the patient is expired
        # if so, check for message (type 0, reason 4)
        # add to messages_no_send if doesnt exist,
        # if there is a message, add to errors

        # check if the patient opted out
        # if so, check for message (type 0, reason 8)
        # add to messages_no_send if doesnt exist,
        # if there is a message, add to errors

        # check if plan is 1 or 3
        # get_phone_db for record

        # check landline to see if you can text
        # check the message db for type 0 reason 7
        # Messages_no_send

        # check message db for type2
        # get survey link
        # insert message_send

        # else:
        # insert messages_errors
        # If plan is 2 or 3
        #! what does this mean? why does it overlap with above?
        # if not check_message_db
        # append to messages_send

        # else:
        # append to messages_errors
        # If there are errors, handle them
        # open s3 bucket, write out csv file with error records using field names
        # Print error message

        # messages = messages_send + messages_no_send (weird.. maybe just boolean?)
        # insert_messages function - inserts into db
        # s3 move from input to processed
        #  success messages
        # message to process messages into pinpoint
        # s3 open file for segment, write file out with channel types ** special flags here #829

        # print message if messages_send > 0
        # initialize client
        # get a create_import_job response to start off segment run
        # catch error and print, else(???):
        # print the IDs

        # print message to update db with segment schedule
        # s3 open bucket read in
        # write segment file id's, skipping first row
        # set reason to 9, add comment to send to pinpoint
        # db update for all messages created

        # message to create pinpoint segment from template names
        # sleep a certain number of seconds (for timeout?)
        # reinitialize client again...?
        # try a client.create_campaign #912
        # catch error message
        # else again..?
        # print message about updating database with campaign created
        # open s3 file, read in
        # skip first row again
        # db update for all messages

        # Else (from 105)
        #  print no messages to process into pinpoint, check error file

        # Create report
        # if messages_send (again?):
        # s3 open
        # for each row, find matching message and parse out data; write to s3 file with ID, Reason, DateTime

        #  print report saved
        # else print no messages to send, no report (wrap this into earlier fork)

        # Wrap ALL OF THAT into an exception as a general error
        #  if there is an error, print error, otherwise print success and return

        pass

    def process_history(self):
        pass

    def process_resend(self):
        pass

    def process_error(self):
        # todo: Create s3 file with erroneous records
        logger.print_and_log(f"Error file saved in folder")
        pass

    def process(self):
        if self.new:
            self.process_new()
        if self.history:
            self.process_history()
        if self.resend:
            self.process_resend()
        if self.error:
            self.process_error()


def setup_log():
    root_path = os.getcwd()
    if settings.log_dir_path:
        log_file_dir = settings.log_dir_path
    else:
        log_file_dir = Path(root_path, "logs")

    logger.start_log(log_path=log_file_dir)


def main(event, context):
    setup_log()
    logger.print_and_log(LINE)
    logger.print_and_log(f"Received event(s): {str(event)}")
    start = time.time()
    logger.print_and_log(
        f"Process beginning at {time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(start))}"
    )
    # Logic for profiling (see what is taking the most time in the process)
    if settings.PROFILE:
        with cProfile.Profile() as pr:
            handler = Handler(event)
            handler.process()
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.dump_stats("profile.prof")
    else:
        handler = Handler(event)
        handler.process()
    end = time.time()
    logger.print_and_log(
        f"Finished at {time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(end))}. Process took {end-start:.3f} seconds."
    )
