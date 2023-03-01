from sqlalchemy import Column, String, Integer, BigInteger, Boolean, ARRAY

from dbsession import Base


class AddressBalance(Base):
    __tablename__ = 'address_balances'
    address = Column(String, primary_key=True)
    balance = Column(BigInteger)