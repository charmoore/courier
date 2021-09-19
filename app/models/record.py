from sqlmodel import SQLModel

class Record(SQLModel):
    SurveyLink, Address, ReasonID, Comment, TypeID, DTGSent, PatientPhone

    pass