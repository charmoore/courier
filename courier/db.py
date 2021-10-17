from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from courier.config import settings
from courier.utils.logging import Logger
from courier.utils.exceptions import DBConnectionError

logger = Logger("base.db")

engine = create_engine(settings.DATABASE_URI, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator:
    "Yield the database session as needed"
    try:
        db = SessionLocal()
        logger.print_and_log("DB Session connected...")
        yield db
    except Exception as e:
        raise DBConnectionError(e.message)
    finally:
        logger.print_and_log("Closing DB connection...")
        db.close()
