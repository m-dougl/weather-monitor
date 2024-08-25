import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from models import Base

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/database.log"),
        logging.StreamHandler(),
    ],
)

DB_PATH = os.getenv("DB_PATH")

if not DB_PATH:
    logging.error("DB_PATH is not set in environment variables.")
    raise ValueError("DB_PATH must be set in environment variables.")

engine = create_engine(DB_PATH, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    logging.info("Initializing the database...")
    try:
        Base.metadata.create_all(bind=engine)
        logging.info("Database initialized successfully.")
    except Exception as e:
        logging.error(f"An error occurred while initializing the database: {e}")
        raise


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
