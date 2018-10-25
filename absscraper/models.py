from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from config import defaults

IndexBase = declarative_base()


class IndexDb(object):
    """
    Database class
    """
    def __init__(self):
        self.db_file = defaults['db_name']
        self.engine = create_engine('sqlite:///'+self.db_file, echo=False)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def setup(self):
        """
        Create database tables.
        :return:
        """
        IndexBase.metadata.create_all(self.engine)

    def clear(self):
        """
        Drop all tables.
        :return:
        """
        IndexBase.metadata.drop_all(self.engine)

    @staticmethod
    @contextmanager
    def get_session():
        """
        Provide a transactional scope around a series of operations.
        :return: session object
        """
        session = IndexDb().Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class Filing(IndexBase):
    """
    Class representing an ABS-EE filing in index.
    """
    __tablename__ = 'filings'

    acc_no = Column(Integer, primary_key=True, unique=True, nullable=False)
    cik_trust = Column(Integer, ForeignKey('companies.cik'))
    cik_filer = Column(Integer)
    url = Column(String(255), nullable=False)
    is_downloaded = Column(Boolean, default=False)
    is_parsed = Column(Boolean, default=False)
    date_filing = Column(Date)
    date_add = Column(DateTime(timezone=True), server_default=func.now())
    date_upd = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # Added at a later stage after data collection had started
    skip = Column(Boolean, default=False)

    trust = relationship("Company", back_populates="filings")

    def __repr__(self):
        return f"<Filing(cik_trust={self.cik_trust}, date={self.date_filing}, acc_no={self.acc_no})>"


class Company(IndexBase):
    """
    Class representing a company (trust or filer) in index.
    """
    __tablename__ = 'companies'

    cik = Column(Integer, primary_key=True, unique=True, nullable=False)
    name = Column(String(255))
    is_trust = Column(Boolean, default=False)
    asset_type = Column(String(32))
    date_add = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<Company(cik={self.cik}, name={self.name}, asset_type={self.asset_type}, " \
               f"is_trust={self.is_trust})>"


Company.filings = relationship("Filing", order_by=Filing.date_filing, back_populates="trust")
