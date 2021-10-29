from typing import List, Optional, Dict, Any

from pydantic import BaseSettings, Field, AnyUrl, validator
import s3fs
import urllib.parse


class Settings(BaseSettings):
    # If you want time profiling data:
    PROFILE: bool = False
    # Project creds
    AWS_REGION: str = Field(..., env="region")
    importRoleArn: str
    projectId: str
    s3: s3fs.S3FileSystem = s3fs.S3FileSystem(anon=False)
    formats: List[str] = [".csv"]

    # Database creds
    db_host: str
    db_database: str
    db_username: str = Field(..., env="db_user")
    db_password: str = Field(..., env="db_pass")

    DATABASE_URI: Optional[AnyUrl] = None

    # Validator to populate URI
    @validator("DATABASE_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        user = values.get("db_username")
        password = urllib.parse.quote_plus(f"{values.get('db_password')}")
        host = values.get("db_host")
        db = values.get("db_database")
        return f"mysql://{user}:{password}@{host}/{db}"

    # Run config
    practice: str = Field(..., env="Practice")
    plan: str = Field(..., env="PlanID")
    age_min: int = Field(default=0)

    # Templates
    template_sms: str = Field(..., env="TemplateSMS")
    template_email: str = Field(..., env="TemplateEmail")

    # Misc
    seconds: int
    log_dir_path: str = None
    LOGGING_DEBUG: bool = False
    LOGGING_FORMAT = "[%(levelname)s - %(name)s][%(asctime)s] %(message)s"
    fieldnames: List[str] = [
        "ChannelType",
        "Address",
        "Id",
        "User.UserAttributes.PracticeName",
        "User.UserAttributes.PatientName",
        "Location.Country",
        "User.UserAttributes.Age",
        "User.UserAttributes.DateOfService",
        "User.UserAttributes.ServicingProvider",
        "User.UserAttributes.LocationName",
        "User.UserAttributes.VisitNumber",
        "User.UserAttributes.PostDate",
        "User.UserAttributes.DateofDeath",
        "User.UserAttributes.MessageID",
        "User.UserAttributes.SurveyLink",
    ]

    class Config:
        case_sensitive = True
        env_file = ".env"


settings = Settings()
