import argparse
import sys
from sqlalchemy import case
from helpers import ats, ok
from assets import *


class AbsHandler(object):
    """
    App class for abs handler.
    """
    def __init__(self, company, rebuild=False, quick=False, ind_trusts=[], ind_filings=[]):
        self.rebuild = rebuild
        self.quick = quick
        self.ind_trusts = ind_trusts
        self.ind_filings = ind_filings
        self.company = company

    def dispatch(self):
        """
        Direct logic flow depending on passed command-line arguments.
        :return: None
        """

        if self.rebuild:
            answer = input("You sure you want to reprocess? [yes/No]? ")
            if answer.lower() == 'yes':
                db = AssetDb()
                try:
                    db.clear_table(AutoloanFlat.__tablename__)
                except:
                    pass
                db.setup_table(AutoloanFlat.__tablename__)
                print(f"{ats()} Flat table cleared...")
            else:
                print(f"{ats()} Aborting...")
                sys.exit(1)

        self.process()

        print(f"{ats()} Finished. Good job!")
        ok()

    def process(self):
        """
        Process raw auto loan data and put it into a "flat" table.
        :return: None
        """
        with AssetDb.get_session() as session:

            print(f'{ats()} Querying database...')
            # Build subquery with different filters depending on passed arguments
            if len(self.ind_filings):
                qs = session.query(Autoloan, AssetFiling.trustCik, AssetFiling.trustName, AssetFiling.dateFiling) \
                    .outerjoin(AssetFiling, Autoloan.filingAccNo == AssetFiling.accNo) \
                    .filter(AssetFiling.accNo.in_(self.ind_filings)).subquery()
            elif len(self.ind_trusts):
                qs = session.query(Autoloan, AssetFiling.trustCik, AssetFiling.trustName, AssetFiling.dateFiling) \
                    .outerjoin(AssetFiling, Autoloan.filingAccNo == AssetFiling.accNo) \
                    .filter(AssetFiling.trustCik.in_(self.ind_trusts)).subquery()
            elif self.company:
                qs = session.query(Autoloan, AssetFiling.trustCik, AssetFiling.trustName, AssetFiling.dateFiling) \
                    .outerjoin(AssetFiling, Autoloan.filingAccNo == AssetFiling.accNo) \
                    .filter(AssetFiling.trustName.like(f'%{self.company}%')).subquery()
            # Build query
            q = session.query(
                func.concat(qs.c.trustCik, "_", qs.c.assetNumber).label('trustAssetNumber'),
                func.min(qs.c.dateFiling).label('dateFirstFiling'),
                func.any_value(qs.c.trustCik).label('trustCik'),
                func.any_value(qs.c.assetNumber).label('assetNumber'),
                func.any_value(qs.c.originationDate).label('originationDate'),
                func.any_value(qs.c.originalLoanAmount).label('originalLoanAmount'),
                func.any_value(qs.c.originalLoanTerm).label('originalLoanTerm'),
                func.any_value(qs.c.loanMaturityDate).label('loanMaturityDate'),
                func.any_value(qs.c.originalInterestRatePercentage).label('originalInterestRatePercentage'),
                func.any_value(qs.c.underwritingIndicator).label('underwritingIndicator'),
                func.any_value(qs.c.gracePeriodNumber).label('gracePeriodNumber'),
                func.any_value(qs.c.subvented).label('subvented'),
                func.any_value(qs.c.vehicleManufacturerName).label('vehicleManufacturerName'),
                func.any_value(qs.c.vehicleModelName).label('vehicleModelName'),
                func.any_value(qs.c.vehicleNewUsedCode).label('vehicleNewUsedCode'),
                func.any_value(qs.c.vehicleModelYear).label('vehicleModelYear'),
                func.any_value(qs.c.vehicleTypeCode).label('vehicleTypeCode'),
                func.any_value(qs.c.vehicleValueAmount).label('vehicleValueAmount'),
                func.any_value(qs.c.obligorCreditScore).label('obligorCreditScore'),
                func.any_value(qs.c.obligorIncomeVerificationLevelCode) \
                    .label('obligorIncomeVerificationLevelCode'),
                func.any_value(qs.c.obligorEmploymentVerificationCode) \
                    .label('obligorEmploymentVerificationCode'),
                func.any_value(qs.c.coObligorIndicator).label('coObligorIndicator'),
                func.any_value(qs.c.paymentToIncomePercentage).label('paymentToIncomePercentage'),
                func.any_value(qs.c.obligorGeographicLocation).label('obligorGeographicLocation'),
                func.min(qs.c.zeroBalanceEffectiveDate).label('zeroBalanceEffectiveDate'),
                func.min(qs.c.zeroBalanceCode).label('zeroBalanceCode'),
                func.min(case(
                    [(qs.c.currentDelinquencyStatus > 30, qs.c.reportingPeriodEndingDate)],
                    else_=None
                    )).label('delinquency30Days'),
                func.min(case(
                    [(qs.c.currentDelinquencyStatus > 90, qs.c.reportingPeriodEndingDate)],
                    else_=None
                    )).label('delinquency90Days'),
                func.max(qs.c.repossessedIndicator).label('repossessedIndicator'),
                func.min(case(
                    [(qs.c.repossessedIndicator > 0, qs.c.reportingPeriodEndingDate)],
                    else_=None
                    )).label('repossessedDate'),
                ) \
                .group_by('trustAssetNumber') \
                .order_by('originationDate')

            if self.quick:
                # Do quick insert using from_select (whole query result is appended to existing table)
                table = AssetBase.metadata.tables[AutoloanFlat.__tablename__]
                session.execute(table.insert().from_select(names=table.columns._data.keys(), select=q))
                print(f'{ats()} Results received! Done quick insert.')
            else:
                # Go line-by-line and insert results one-by-one (slow), but necessary for update
                field_names = [c['name'] for c in q.column_descriptions]
                loan_counter = 0
                for row in q:
                    # Display message only when results received
                    if loan_counter == 0:
                        print(f'{ats()} Results received! Start processing...')

                    # Build dict of field-value pairs
                    row_d = {f: v for f, v in zip(field_names, row)}
                    # Construct unique loan identifier (string)
                    trustAssetNumber = str(row.trustCik) + "_" + str(row.assetNumber)

                    # Retrieve object from db
                    flatloan = session.query(AutoloanFlat).get(trustAssetNumber)
                    if flatloan is None:
                        # Create new object if record does not exist
                        flatloan = AutoloanFlat(trustAssetNumber=trustAssetNumber)
                        for field, value in row_d.items():
                            setattr(flatloan, field, value)
                        session.add(flatloan)
                    else:
                        # Create new object if record does not exist
                        for field, value in row_d.items():
                            # Only update field if they are empty
                            if field in flatloan.updatable_fields:
                                if getattr(flatloan, field) is None:
                                    setattr(flatloan, field, value)

                    loan_counter += 1
                    print(f'Processed {loan_counter} records...', end="\r")
                print("")


def main():

    ap = argparse.ArgumentParser(description="Utility for preparation of ABS-EE data.")

    ap.add_argument("-r", "--rebuild", required=False, action='store_true', default=False,
                    help="redo everything from scratch")
    ap.add_argument("-q", "--quick", required=False, action='store_true', default=False,
                    help="use quick insert from_select instead of slow row-by-row insert")
    ap.add_argument("-t", "--trust", required=False, type=str,
                    help="trust ciks separated by ':'")
    ap.add_argument("-f", "--filing", required=False, type=str,
                    help="filing accession numbers separated by ':'")
    ap.add_argument("-c", "--company", required=False, type=str,
                    help="company name")

    args = vars(ap.parse_args())

    # Check if both trust and filing arguments were passed
    if args['trust'] is not None and args['filing'] is not None:
        print("Cannot use both trust and filing options.")
        ap.print_help()
        sys.exit(2)
    elif args['trust'] is not None and args['company'] is not None:
        print("Cannot use both trust and brand options.")
        ap.print_help()
        sys.exit(2)
    elif args['filing'] is not None and args['company'] is not None:
        print("Cannot use both filing and brand options.")
        ap.print_help()
        sys.exit(2)

    ind_trusts = []
    if args['trust'] is not None:
        ind_trusts = list(map(lambda x: int(x), args['trust'].split(":")))
    ind_filings = []
    if args['filing'] is not None:
        ind_filings = list(map(lambda x: int(x), args['filing'].split(":")))

    # Initiate and run parser
    abs_handler = AbsHandler(args['company'], args['rebuild'], args['quick'], ind_trusts, ind_filings)
    abs_handler.dispatch()


if __name__ == '__main__':
    main()
