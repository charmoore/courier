from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import settings
from utils.logging import Logger

logger = Logger("base.db")

engine=create_engine(settings.DATABASE_URI, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator:
    "Yield the database session as needed"
    try: 
        db = SessionLocal()
        yield db
    except Exception as e:
        logger.print_and_log("Could not connect to database.", log_level="error")
    finally: 
        logger.print_and_log("Closing db connection...")
        db.close()

