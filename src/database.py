import os
from logger import Logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import Base

load_dotenv()
logger = Logger(source="database")

DB_PATH = os.getenv("DB_PATH")

if not DB_PATH:
    logger.log(level='error', message="DB_PATH is not set in environment variables.")
    raise ValueError("DB_PATH must be set in environment variables.")

engine = create_engine(DB_PATH, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    logger.log(level='info', message='Initializing the database...')
    try:
        Base.metadata.create_all(bind=engine)
        logger.log(level='info', message='Database initialized successfully.')
    except Exception as e:
        logger.log(level='error', message='An error occurred while initializing the database: {e}')
        raise ValueError(f"An error occurred while initializing the database: {e}")


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
