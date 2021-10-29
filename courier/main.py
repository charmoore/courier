import os
from pathlib import Path
import pandas as pd
import time
from fastapi import Depends
import cProfile
import pstats
import traceback
import datetime
import boto3
from botocore.exceptions import ClientError

from courier.config import settings
from courier.db import get_db
from courier.utils.logging import Logger
from courier.utils.enums import Reasons, MessageTypes
from courier.models.runner import build as Runner
from courier.crud import crud
from courier.models import Patients, Visits, Locations, Messages, MessagesSend
from courier.models.rootrunner import RootRunner
from courier.utils.utils import (
    convert_sql_datetime,
    valid_phone,
    check_date_service,
    get_reason_message,
    s3_write_df,
)
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
                            if int(row["Age"]) < settings.age_min:
                                message = Messages(
                                    ReasonID=Reasons.UNDER_AGE.value,
                                    Comment=get_reason_message(Reasons.UNDER_AGE),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)
                            death = None
                            if row["DateOfDeath"]:
                                death = convert_sql_datetime(row["DateOfDeath"])
                                message = Messages(
                                    ReasonID=Reasons.EXPIRED.value,
                                    Comment=get_reason_message(Reasons.EXPIRED),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)
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
                                Death=death,
                            )
                            root_runner.patients.append(patient)
                        else:
                            patient = crud.patients.get(db=db, id=row["PatientID"])
                            if patient.OptOut == 1:
                                message = Messages(
                                    ReasonID=Reasons.OPTED_OUT.value,
                                    Comment=get_reason_message(Reasons.OPTED_OUT),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)
                        if not crud.visits.exists(db=db, id=row["SurveyRequestID"]):
                            date_of_service = (
                                convert_sql_datetime(row["DateOfService"])
                                if row["DateOfService"]
                                else None
                            )
                            # Check date of service
                            if check_date_service(row["DateOfService"]):
                                message = Messages(
                                    ReasonID=Reasons.ARCHIVE.value,
                                    Comment=get_reason_message(Reasons.ARCHIVE),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)
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
                                DateOfService=date_of_service,
                                DatePosted=post_date,
                                VisitNumber=row["VisitNumber"],
                            )
                            root_runner.visits.append(visit)  # append as list of models
                        if not crud.locations.exists(db=db, id=row["LocationName"]):
                            if "(" in row["LocationName"]:
                                location_id = (
                                    row["LocationName"]
                                    .partition(" ")[0]
                                    .replace("(", "")
                                    .replace(")", "")
                                )
                                location = Locations(
                                    LocationID=location_id,
                                )
                            else:
                                location = Locations(LocationName=row["LocationName"])
                            root_runner.locations.append(
                                location
                            )  # append as list of models
                        if settings.plan == 1:
                            if crud.patients.has_landline(db=db, id=row["PatientID"]):
                                message = Messages(
                                    ReasonID=Reasons.PHONE_INVALID.value,
                                    Comment=get_reason_message(Reasons.PHONE_INVALID),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)

                            address = crud.patients.get(
                                db=db, id=row[["PatientID"]]
                            ).Phone
                            message = Messages(
                                SurveyLink=row["SurveyRequestID"],
                                Address=address,
                                ReasonID=Reasons.PENDING.value,
                                Comment=get_reason_message(Reasons.PENDING),
                                TypeID=MessageTypes.INITIAL_SMS.value,
                                DTGSent=datetime.datetime.today(),
                            )
                            root_runner.messages_no_send.append(message)
                        if settings.plan == 2:
                            message = MessagesSend(
                                SurveyLink=crud.providers.get_survey_link(
                                    db=db,
                                    servicing_provider=row["ServicingProvider"],
                                    request_id=row["SurveyRequestID"],
                                ),
                                Address=crud.patients.get(
                                    db=db, id=row["PatientID"]
                                ).Email,
                                ReasonID=Reasons.PENDING.value,
                                Comment=get_reason_message(Reasons.PENDING),
                                TypeID=MessageTypes.INITIAL_EMAIL.value,
                                DTGSent=datetime.datetime.today(),
                                SurveyRequestID=row["SurveyRequestID"],
                            )
                            root_runner.messages_send.append(message)

                        if settings.plan == 3:
                            if crud.patients.has_landline(db=db, id=row["PatientID"]):
                                message = Messages(
                                    ReasonID=Reasons.PHONE_INVALID.value,
                                    Comment=get_reason_message(Reasons.PHONE_INVALID),
                                    TypeID=MessageTypes.NULL.value,
                                    DTGSent=datetime.datetime.today(),
                                )
                                root_runner.messages_no_send.append(message)

                            address = crud.patients.get(
                                db=db, id=row[["PatientID"]]
                            ).Phone
                            message = Messages(
                                SurveyLink=row["SurveyRequestID"],
                                Address=address,
                                ReasonID=Reasons.PENDING.value,
                                Comment=get_reason_message(Reasons.PENDING),
                                TypeID=MessageTypes.INITIAL_SMS.value,
                                DTGSent=datetime.datetime.today(),
                            )
                            root_runner.messages_no_send.append(message)
                            message = Messages(
                                SurveyLink=crud.providers.get_survey_link(
                                    db=db,
                                    servicing_provider=row["ServicingProvider"],
                                    request_id=row["SurveyRequestID"],
                                ),
                                Address=crud.patients.get(
                                    db=db, id=row["PatientID"]
                                ).Email,
                                ReasonID=Reasons.PENDING.value,
                                Comment=get_reason_message(Reasons.PENDING),
                                TypeID=MessageTypes.INITIAL_EMAIL.value,
                                DTGSent=datetime.datetime.today(),
                            )
                            message.SurveyRequestID = row["SurveyRequestID"]
                            root_runner.messages_send.append(message)
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

                if root_runner.messages_no_send:
                    # try to create each record, catch errors back to here and append to root_runner.messages_errors
                    for message in root_runner.messages_no_send:
                        if not crud.messages.exists(
                            db=db, type=message.TypeID, reason=message.ReasonID
                        ):
                            crud.messages.create(db=db, db_obj=message)
                        else:
                            root_runner.messages_errors.append(message)
                if root_runner.messages_send:
                    for message in root_runner.messages_send:
                        if not crud.messages.exists(
                            db=db, type=message.TypeID, reason=message.ReasonID
                        ):
                            crud.messages.create(db=db, db_obj=message)
                        else:
                            root_runner.messages_errors.append(message)
                if root_runner.messages_errors:
                    path = f"s3://{runner.bucket}/error/{runner.file_errors}"
                    df = pd.DataFrame(
                        [model.dict() for model in root_runner.messages_errors]
                    )
                    s3_write_df(data=df, path=path)
                    logger.print_and_log(
                        f"Some records contained errors. They were saved to {path}",
                        "warning",
                    )
                # Move file to processed folder
                move_from = f"s3://{runner.bucket}/{runner.file_in}"
                move_to = f"s3://{runner.bucket}/processed/{runner.file_processed}"
                settings.s3.move(move_from, move_to)

                # Send over to messages for segment handling
                segment = crud.messages_pending.build_segment(
                    db=db, root_runner=root_runner, runner=runner
                )
                segment_path = f"s3://{runner.bucket}/segment/{runner.file_out}"
                s3_write_df(segment, segment_path)

                # Send the segment
                if root_runner.messages_send:
                    logger.print_and_log(f"Sending messages to Pinpoint...")
                    client = boto3.client("pinpoint")
                    response = client.create_import_job(
                        ApplicationId=settings.projectId,
                        ImportJobRequest={
                            "DefineSegment": True,
                            "Format": "CSV",
                            "RegisterEndpoints": True,
                            "RoleArn": settings.importRoleArn,
                            "S3Url": f"s3://{runner.bucket}/segment/{runner.file_out}",
                            "SegmentName": runner.segment_name,
                        },
                    )
                    segment_id = response["ImportJobResponse"]["Definition"][
                        "SegmentId"
                    ]
                    logger.print_and_log(
                        "Import job "
                        + response["ImportJobResponse"]["Id"]
                        + " "
                        + response["ImportJobResponse"]["JobStatus"]
                        + "."
                    )
                    logger.print_and_log(
                        "Segment ID: "
                        + response["ImportJobResponse"]["Definition"]["SegmentId"]
                    )
                    logger.print_and_log("Application ID: " + settings.projectId)
                    logger.print_and_log(
                        f"Updating {settings.db_database} database with segment schedule."
                    )
                    # Update all sent messages in db
                    for message in root_runner.messages_send:
                        crud.messages.update(
                            db=db,
                            db_obj=message,
                            obj_in={
                                "Reason": Reasons.SEGMENT_PENDING.value,
                                "Comment": get_reason_message(Reasons.SEGMENT_PENDING),
                            },
                        )
                    logger.print_and_log(
                        f"Creating Pinpoint campaign from segment {runner.segment_name} (id: {segment_id})."
                    )
                    logger.print_and_log(
                        f"Using templates: \n {settings.template_sms} \n {settings.template_email}"
                    )
                    # Todo ask about this sleep?
                    pinpoint_client = boto3.client("pinpoint")
                    response = pinpoint_client.create_campaign(
                        ApplicationId=settings.projectId,
                        WriteCampaignRequest={
                            "Description": "Campaign created to send Clearsurvey surveys to patients after their visit to a provider.",
                            "AdditionalTreatments": [],
                            "IsPaused": False,
                            "Schedule": {
                                "Frequency": "ONCE",
                                "IsLocalTime": False,
                                "StartTime": "IMMEDIATE",
                                "Timezone": "UTC",
                                "QuietTime": {},
                            },
                            "TemplateConfiguration": {
                                "EmailTemplate": {"Name": settings.template_email},
                                "SMSTemplate": {"Name": settings.template_sms},
                            },
                            "Name": runner.segment_name,
                            "SegmentId": segment_id,
                            "SegmentVersion": 1,
                        },
                    )
                    logger.print_and_log(f"Response: \n {response}")
                    for message in root_runner.messages_send:
                        crud.messages.update(
                            db=db,
                            db_obj=message,
                            obj_in={
                                "Sent": 1,
                                "ReasonID": Reasons.SENT.value,
                                "Comment": get_reason_message(Reasons.SENT),
                                "DTGSent": {
                                    datetime.datetime.strftime(
                                        datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
                                    )
                                },
                            },
                        )
                else:
                    logger.print_and_log(
                        f"No messages to process to Pinpoint. Check error file",
                        "warning",
                    )
                # Bypassing reporting func

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
    logger.print_and_log(f"Using context: {context}")
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
