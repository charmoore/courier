from typing import Any, Optional

from sqlalchemy.orm import Session
import pandas as pd
import uuid

from courier.crud.crud_base import CRUDBase
from courier.models import (
    Locations,
    Messages,
    Patients,
    Plans,
    Providers,
    Responses,
    Visits,
    MessagesSend,
    RootRunner,
    Runner,
)
from courier.config import settings
from courier.utils.enums import Reasons, MessageTypes


class CRUDLocations(CRUDBase[Locations]):
    def get(self, db: Session, id: Any) -> Optional[Locations]:
        return db.query(self.model).filter(self.model.LocationID == id).first()

    def exists(self, db: Session, id: Any) -> bool:
        if "(" in id:
            id = id.partition(" ")[0].replace("(", "").replace(")", "")
            return self.get(db=db, id=id) is not None
        else:
            return db.query(self.model).filter(self.model.LocationName == id).first()


locations = CRUDLocations(Locations)


class CRUDMessages(CRUDBase[Messages]):
    def get(self, db: Session, id: Any) -> Optional[Messages]:
        return db.query(self.model).filter(self.model.MessageID == id).first()

    def exists(self, db: Session, id: Any, type: int = 0, reason: int = None) -> bool:
        if reason:
            return (
                db.query(self.model)
                .filter(self.model.VisitID == id)
                .filter(self.model.TypeID == type)
                .filter(self.model.ReasonID == reason)
                .first()
                is not None
            )
        return (
            db.query(self.model)
            .filter(self.model.VisitID == id)
            .filter(self.model.TypeID == type)
            .first()
            is not None
        )


messages = CRUDMessages(Messages)


class CRUDPatients(CRUDBase[Patients]):
    def get(self, db: Session, id: Any) -> Optional[Patients]:
        return db.query(self.model).filter(self.model.PatientID == id).first()

    def has_landline(self, db: Session, id: Any) -> bool:
        patient = self.get(db=db, id=id)
        return patient.PhoneType == 2


patients = CRUDPatients(Patients)


class CRUDPlans(CRUDBase[Plans]):
    def get(self, db: Session, id: Any) -> Optional[Plans]:
        return db.query(self.model).filter(self.model.PlanID == id).first()


plans = CRUDPlans(Plans)


class CRUDProviders(CRUDBase[Providers]):
    def get(self, db: Session, id: Any) -> Optional[Providers]:
        return db.query(self.model).filter(self.model.ProviderID == id).first()

    def get_by_names(self, db: Session, name: str) -> Optional[Providers]:
        last_name, first_middle = name.split(",")
        if first_middle.contains(" "):
            first_name, middle_name = first_middle.split(" ")
        else:
            first_name = first_middle
            middle_name = ""
        provider = (
            db.query(self.model)
            .filter(self.model.ProviderFirst == first_name)
            .filter(self.model.ProviderLast == last_name)
            .first()
        )
        return provider

    def get_survey_link(
        self, db: Session, servicing_provider: str, request_id: str
    ) -> str:
        provider = self.get_by_names(db=db, name=servicing_provider)
        return f"{provider.SurveyURL}?id={request_id}"


providers = CRUDProviders(Providers)


class CRUDResponses(CRUDBase[Responses]):
    def get(self, db: Session, id: Any) -> Optional[Responses]:
        return db.query(self.model).filter(self.model.ID == id).first()


responses = CRUDResponses(Responses)


class CRUDVisits(CRUDBase[Visits]):
    def get(self, db: Session, id: Any) -> Optional[Visits]:
        return db.query(self.model).filter(self.model.VisitID == id).first()


visits = CRUDVisits(Visits)


class CRUDViewMessagesPending:
    def get(self, db: Session, visit_id: str, type_id: str) -> Optional[MessagesSend]:
        return (
            db.query(MessagesSend)
            .filter(MessagesSend.VisitID == visit_id)
            .filter(MessagesSend.TypeID == type_id)
            .first()
        )

    def build_segment(
        self, db: Session, rootrunner: RootRunner, runner: Runner
    ) -> pd.DataFrame:
        output = pd.DataFrame(columns=settings.fieldnames)
        for message in rootrunner.messages_send:
            # Todo check with Will on if this obj is correct?
            obj = self.get(
                db=db, visit_id=message.SurveyRequestID, type_id=message.TypeID
            )
            channel_type = "EMAIL" if obj.TypeID == 3 else "SMS"
            temp = {
                "ChannelType": channel_type,
                "Address": obj.Address,
                "Id": str(uuid.uuid4()),
                "User.UserAttributes.PracticeName": settings.practice,
                "User.UserAttributes.PatientName": obj.Patient,
                "Location.Country": "US",
                "User.UserAttributes.Age": obj.Age,
                "User.UserAttributes.DateOfService": obj.DateOfService,
                "User.UserAttributes.ServicingProvider": obj.Provider,
                "User.UserAttributes.LocationName": obj.Location,
                "User.UserAttributes.VisitNumber": obj.SurveyRequestID,
                "User.UserAttributes.PostDate": obj.PostDate,
                "User.UserAttributes.DateofDeath": obj.DateOfDeath,
                "User.UserAttributes.MessageID": obj.MessageID,
                "User.UserAttributes.SurveyLink": obj.SurveyLink,
            }
            output.append(temp)

        return output


messages_pending = CRUDViewMessagesPending()
