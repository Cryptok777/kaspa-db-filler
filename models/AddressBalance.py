from sqlalchemy import TIMESTAMP, Column, String, BigInteger

from dbsession import Base


class AddressBalance(Base):
    __tablename__ = 'address_balances'
    address = Column(String, primary_key=True)
    balance = Column(BigInteger)
    updated_at = Column(TIMESTAMP(timezone=False))
