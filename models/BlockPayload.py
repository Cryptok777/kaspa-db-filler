from sqlalchemy import (
    Column,
    String,
    TIMESTAMP,
)

from dbsession import Base


class BlockPayload(Base):
    __tablename__ = "block_payloads"

    hash = Column(String, primary_key=True)
    payload = Column(String)
    timestamp = Column(TIMESTAMP(timezone=False))
