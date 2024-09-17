import logging
import os

from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

_logger = logging.getLogger(__name__)

load_dotenv(override=True)

engine = create_engine(os.getenv("SQL_URI"), echo=False)
Base = declarative_base()

session_maker = sessionmaker(engine)


async_engine = create_async_engine(
    os.getenv("SQL_ASYNC_URI"),
    pool_size=20,
    max_overflow=30,
    pool_timeout=30,
    pool_recycle=1800,
)
async_session_maker = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


def create_all(drop=False):
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
