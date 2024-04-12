from sqlalchemy import Integer, String, Column, Boolean, ForeignKey, DateTime, Float

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker, scoped_session

engine = create_engine('mysql+pymysql://root:12345678@127.0.0.1:3306/classification_platform')
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

Base = declarative_base()
metadata = Base.metadata


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(255))
    firstname = Column(String(255))
    lastname = Column(String(255))
    email = Column(String(255))
    password = Column(String(255))
    phone = Column(String(255))


class Token(Base):
    __tablename__ = 'token'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='SET NULL'))
    user = relationship('User')
    personal_token = Column(String(255))
    amount_of_processing_records_left = Column(Integer)
    creating_timestamp = Column(DateTime)


class Voucher(Base):
    __tablename__ = 'voucher'
    id = Column(Integer, primary_key=True)
    voucher_string = Column(String(255))
    amount_of_processing_records_to_add = Column(Integer)
    voucher_is_active = Column(Boolean)
    voucher_used_by_user_id = Column(Integer, ForeignKey('user.id', ondelete='SET NULL'))
    user = relationship('User')
    voucher_created_timestamp = Column(DateTime)
    voucher_used_timestamp = Column(DateTime)


class TransactionHistory(Base):
    __tablename__ = 'transaction_history'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='SET NULL'))
    user = relationship('User')
    token_id = Column(Integer, ForeignKey('token.id', ondelete='SET NULL'))
    token = relationship('Token')
    voucher_id = Column(Integer, ForeignKey('voucher.id', ondelete='SET NULL'))
    voucher = relationship('Voucher')
    amount_of_processing_records_to_add = Column(Integer)
    transaction_amount = Column(Float)
    transaction_timestamp = Column(DateTime)


class ProcessingHistory(Base):
    __tablename__ = 'processing_history'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='SET NULL'))
    user = relationship('User')
    token_id = Column(Integer, ForeignKey('token.id', ondelete='SET NULL'))
    token = relationship('Token')
    amount_of_processed_records = Column(Integer)
    description = Column(String(255))
    processing_timestamp = Column(DateTime)