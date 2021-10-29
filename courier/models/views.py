from sqlalchemy import Table
from sqlmodel import SQLModel

from courier.db import engine

metadata = SQLModel.metadata

MessagesPending = Table("viewMessagesPending", metadata, autoload_with=engine)
