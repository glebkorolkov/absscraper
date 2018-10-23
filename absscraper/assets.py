from config import db_config
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, DateTime, Date
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from sqlalchemy.types import DECIMAL

AssetBase = declarative_base()


class AssetDb(object):
    """
    Database class for parser.
    """
    def __init__(self):
        self.db_config = db_config
        engine_uri = "{0}+pymysql://{1}:{2}@{3}:{4}/{5}".format(
            db_config['db_type'],
            db_config['db_user'], db_config['db_password'], db_config['db_host'],
            db_config['db_port'], db_config['db_name'])
        self.engine = create_engine(engine_uri, echo=False)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def setup(self):
        """
        Create database tables.
        :return:
        """
        AssetBase.metadata.create_all(self.engine)

    def clear(self):
        """
        Drop all tables.
        :return:
        """
        AssetBase.metadata.drop_all(self.engine)

    @staticmethod
    @contextmanager
    def get_session():
        """
        Provide a transactional scope around a series of operations.
        :return: session object
        """
        session = AssetDb().Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class Autoloan(AssetBase):
    """
    Auto loan records class.
    """
    __tablename__ = 'autoloans'

    autoloanId = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    filingAccNo = Column(BigInteger, nullable=False)
    assetTypeNumber = Column(String(100))
    assetNumber = Column(String(25))
    reportingPeriodBeginningDate = Column(Date)
    reportingPeriodEndingDate = Column(Date)
    originatorName = Column(String(50))
    originationDate = Column(Date)
    originalLoanAmount = Column(DECIMAL(20, 8))
    originalLoanTerm = Column(Integer)
    loanMaturityDate = Column(Date)
    originalInterestRatePercentage = Column(DECIMAL(20, 8))
    interestCalculationTypeCode = Column(String(255))
    originalInterestRateTypeCode = Column(String(255))
    originalInterestOnlyTermNumber = Column(Integer)
    originalFirstPaymentDate = Column(Date)
    underwritingIndicator = Column(Boolean)
    gracePeriodNumber = Column(Integer)
    paymentTypeCode = Column(String(255))
    subvented = Column(String(255))
    vehicleManufacturerName = Column(String(30))
    vehicleModelName = Column(String(30))
    vehicleNewUsedCode = Column(String(255))
    vehicleModelYear = Column(String(4))
    vehicleTypeCode = Column(String(255))
    vehicleValueAmount = Column(DECIMAL(20, 8))
    vehicleValueSourceCode = Column(String(255))
    obligorCreditScoreType = Column(String(35))
    obligorCreditScore = Column(String(20))
    obligorIncomeVerificationLevelCode = Column(String(255))
    obligorEmploymentVerificationCode = Column(String(255))
    coObligorIndicator = Column(Boolean)
    paymentToIncomePercentage = Column(DECIMAL(20, 8))
    obligorGeographicLocation = Column(String(100))
    assetAddedIndicator = Column(Boolean)
    remainingTermToMaturityNumber = Column(Integer)
    reportingPeriodModificationIndicator = Column(Boolean)
    servicingAdvanceMethodCode = Column(String(255))
    reportingPeriodBeginningLoanBalanceAmount = Column(DECIMAL(20, 8))
    nextReportingPeriodPaymentAmountDue = Column(DECIMAL(20, 8))
    reportingPeriodInterestRatePercentage = Column(DECIMAL(20, 8))
    nextInterestRatePercentage = Column(DECIMAL(20, 8))
    servicingFeePercentage = Column(DECIMAL(20, 8))
    servicingFlatFeeAmount = Column(DECIMAL(20, 8))
    otherServicerFeeRetainedByServicer = Column(DECIMAL(20, 8))
    otherAssessedUncollectedServicerFeeAmount = Column(DECIMAL(20, 8))
    scheduledInterestAmount = Column(DECIMAL(20, 8))
    scheduledPrincipalAmount = Column(DECIMAL(20, 8))
    otherPrincipalAdjustmentAmount = Column(DECIMAL(20, 8))
    reportingPeriodActualEndBalanceAmount = Column(DECIMAL(20, 8))
    reportingPeriodScheduledPaymentAmount = Column(DECIMAL(20, 8))
    totalActualAmountPaid = Column(DECIMAL(20, 8))
    actualInterestCollectedAmount = Column(DECIMAL(20, 8))
    actualPrincipalCollectedAmount = Column(DECIMAL(20, 8))
    actualOtherCollectedAmount = Column(DECIMAL(20, 8))
    servicerAdvancedAmount = Column(DECIMAL(20, 8))
    interestPaidThroughDate = Column(Date)
    zeroBalanceEffectiveDate = Column(Date)
    zeroBalanceCode = Column(String(255))
    currentDelinquencyStatus = Column(Integer)
    primaryLoanServicerName = Column(String(100))
    mostRecentServicingTransferReceivedDate = Column(Date)
    assetSubjectDemandIndicator = Column(Boolean)
    assetSubjectDemandStatusCode = Column(String(255))
    repurchaseAmount = Column(DECIMAL(20, 8))
    demandResolutionDate = Column(Date)
    repurchaserName = Column(String(30))
    repurchaseReplacementReasonCode = Column(String(255))
    chargedoffPrincipalAmount = Column(DECIMAL(20, 8))
    recoveredAmount = Column(DECIMAL(20, 8))
    modificationTypeCode = Column(String(255))
    paymentExtendedNumber = Column(Integer)
    repossessedIndicator = Column(Boolean)
    repossessedProceedsAmount = Column(DECIMAL(20, 8))
    dateAdd = Column(DateTime(timezone=True), server_default=func.now())

    # Fields requiring preprocessing
    special_fields = {
        'reportingPeriodBeginningDate': 'Date1',
        'reportingPeriodEndingDate': 'Date1',
        'originationDate': 'Date2',
        'loanMaturityDate': 'Date2',
        'originalFirstPaymentDate': 'Date2',
        'underwritingIndicator': 'Boolean',
        'subvented': 'Unlimited',
        'coObligorIndicator': 'Boolean',
        'assetAddedIndicator': 'Boolean',
        'reportingPeriodModificationIndicator': 'Boolean',
        'interestPaidThroughDate': 'Date1',
        'zeroBalanceEffectiveDate': 'Date2',
        'zeroBalanceCode': 'Unlimited',
        'mostRecentServicingTransferReceivedDate': 'Date2',
        'assetSubjectDemandIndicator': 'Boolean',
        'demandResolutionDate': 'Date1',
        'repurchaseReplacementReasonCode': 'Unlimited',
        'modificationTypeCode': 'Unlimited',
        'repossessedIndicator': 'Boolean'
    }

    def __repr__(self):
        return f"<Autoloan(autoloanId={self.autoloanId}, filingAccNo={self.filingAccNo})>"


class Autolease(AssetBase):
    """
    Auto lease records class.
    """
    __tablename__ = 'autoleases'

    autoleaseId = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    filingAccNo = Column(BigInteger, nullable=False)
    assetTypeNumber = Column(String(255))
    assetNumber = Column(String(255))
    reportingPeriodBeginDate = Column(Date)
    reportingPeriodEndDate = Column(Date)
    originatorName = Column(String(255))
    originationDate = Column(Date)
    acquisitionCost = Column(DECIMAL(20, 8))
    originalLeaseTermNumber = Column(Integer)
    scheduledTerminationDate = Column(Date)
    originalFirstPaymentDate = Column(Date)
    underwritingIndicator = Column(Boolean)
    gracePeriod = Column(Integer)
    paymentTypeCode = Column(String(255))
    subvented = Column(String(255))
    vehicleManufacturerName = Column(String(255))
    vehicleModelName = Column(String(255))
    vehicleNewUsedCode = Column(String(255))
    vehicleModelYear = Column(String(255))
    vehicleTypeCode = Column(String(255))
    vehicleValueAmount = Column(DECIMAL(20, 8))
    vehicleValueSourceCode = Column(String(255))
    baseResidualValue = Column(DECIMAL(20, 8))
    baseResidualSourceCode = Column(String(255))
    contractResidualValue = Column(DECIMAL(20, 8))
    lesseeCreditScoreType = Column(String(255))
    lesseeCreditScore = Column(String(255))
    lesseeIncomeVerificationLevelCode = Column(String(255))
    lesseeEmploymentVerificationCode = Column(String(255))
    coLesseePresentIndicator = Column(Boolean)
    paymentToIncomePercentage = Column(DECIMAL(20, 8))
    lesseeGeographicLocation = Column(String(255))
    assetAddedIndicator = Column(Boolean)
    remainingTermNumber = Column(Integer)
    reportingPeriodModificationIndicator = Column(Boolean)
    servicingAdvanceMethodCode = Column(String(255))
    reportingPeriodSecuritizationValueAmount = Column(DECIMAL(20, 8))
    securitizationDiscountRate = Column(DECIMAL(20, 8))
    nextReportingPeriodPaymentAmountDue = Column(DECIMAL(20, 8))
    servicingFeePercentage = Column(DECIMAL(20, 8))
    servicingFlatFeeAmount = Column(DECIMAL(20, 8))
    otherLeaseLevelServicingFeesRetainedAmount = Column(DECIMAL(20, 8))
    otherAssessedUncollectedServicerFeeAmount = Column(DECIMAL(20, 8))
    reportingPeriodEndingActualBalanceAmount = Column(DECIMAL(20, 8))
    reportingPeriodScheduledPaymentAmount = Column(DECIMAL(20, 8))
    totalActualAmountPaid = Column(DECIMAL(20, 8))
    actualOtherCollectedAmount = Column(DECIMAL(20, 8))
    reportingPeriodEndActualSecuritizationAmount = Column(DECIMAL(20, 8))
    servicerAdvancedAmount = Column(DECIMAL(20, 8))
    paidThroughDate = Column(Date)
    zeroBalanceEffectiveDate = Column(Date)
    zeroBalanceCode = Column(String(255))
    currentDelinquencyStatus = Column(Integer)
    primaryLeaseServicerName = Column(String(255))
    mostRecentServicingTransferReceivedDate = Column(Date)
    assetSubjectDemandIndicator = Column(Boolean)
    assetSubjectDemandStatusCode = Column(String(255))
    repurchaseAmount = Column(DECIMAL(20, 8))
    DemandResolutionDate = Column(Date)
    repurchaserName = Column(String(255))
    repurchaseOrReplacementReasonCode = Column(String(255))
    chargedOffAmount = Column(DECIMAL(20, 8))
    modificationTypeCode = Column(String(255))
    leaseExtended = Column(Integer)
    terminationIndicator = Column(String(255))
    excessFeeAmount = Column(DECIMAL(20, 8))
    liquidationProceedsAmount = Column(DECIMAL(20, 8))
    dateAdd = Column(DateTime(timezone=True), server_default=func.now())

    # Fields requiring preprocessing
    special_fields = {
        'reportingPeriodBeginningDate': 'Date1',
        'reportingPeriodEndingDate': 'Date1',
        'originationDate': 'Date2',
        'scheduledTerminationDate': 'Date2',
        'originalFirstPaymentDate': 'Date2',
        'underwritingIndicator': 'Boolean',
        'subvented': 'Unlimited',
        'coLesseePresentIndicator': 'Boolean',
        'assetAddedIndicator': 'Boolean',
        'reportingPeriodModificationIndicator': 'Boolean',
        'paidThroughDate': 'Date1',
        'zeroBalanceEffectiveDate': 'Date2',
        'zeroBalanceCode': 'Unlimited',
        'mostRecentServicingTransferReceivedDate': 'Date2',
        'assetSubjectDemandIndicator': 'Boolean',
        'demandResolutionDate': 'Date1',
        'repurchaseOrReplacementReasonCode': 'Unlimited',
        'modificationTypeCode': 'Unlimited',
        'terminationIndicator': 'Unlimited'
    }

    def __repr__(self):
        return f"<Autolease(autoleaseId={self.autoleaseId}, filingAccNo={self.filingAccNo})>"


class AssetFiling(AssetBase):
    """
    Filing class.
    """
    __tablename__ = 'filings'

    accNo = Column(Integer, primary_key=True, unique=True, nullable=False)
    trustCik = Column(Integer)
    trustName = Column(String(255))
    url = Column(String(255), nullable=False)
    dateFiling = Column(Date)
    assetType = Column(String(32))
    dateAdd = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<AssetFiling(dateFiling={self.dateFiling}, trustName={self.trustName}, acc_no={self.accNo})>"

