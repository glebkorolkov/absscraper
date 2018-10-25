import argparse
import sys
import os
import boto3
import re
from lxml import etree
from datetime import date
from helpers import ats, ok
from assets import *
from models import *


class AbsParser(object):
    """
    Class for the xml parser app.
    """
    def __init__(self, warn=False, rebuild=False, use_s3=False,  n_limit=0,
                 asset_types={'autoloan', 'autolease'}, ind_trusts=[], ind_filings=[],
                 output='csv'):
        self.warn = warn
        self.use_s3 = use_s3
        self.rebuild = rebuild
        self.n_limit = n_limit
        self.asset_types = asset_types
        self.ind_trusts = ind_trusts
        self.ind_filings = ind_filings
        self.output = output

    def dispatch(self):
        """
        Direct logic flow depending on passed command-line arguments.
        :return: None
        """
        if self.warn:
            self.issue_warnings()
            print(f'{ats()} Done!')
            sys.exit(1)

        if self.rebuild:
            answer = input("You sure you want to reparse? [yes/No]? ")
            if answer.lower() == 'yes':
                db = AssetDb()
                db.clear()
                db.setup()
                print(f"{ats()} Index cleared...")
            else:
                print(f"{ats()} Aborting...")
                sys.exit(1)

        self.parse()

        print(f"{ats()} Finished. Good job!")
        ok()

    def issue_warnings(self):
        """
        Compile a list of filings filed on the same date (skipping those with skip flag in db.
        :return: None (print output to stdout)
        """
        with IndexDb.get_session() as session:
            multifilings = session.query(Company.name.label('trust'), Filing.cik_trust, Filing.date_filing,
                              func.count().label('num_filings')) \
                .filter(Filing.skip == False) \
                .filter(Filing.cik_trust == Company.cik) \
                .filter(Company.asset_type.in_(self.asset_types))\
                .order_by(Company.name)\
                .group_by(Filing.cik_trust, Filing.date_filing) \
                .having(func.count() > 1).all()
            if len(multifilings) > 0:
                print(f"{ats()} Please, check the following filings that occured on same day:")
                for m in multifilings:
                    print(f'{m.trust} (cik: {m.cik_trust}) on {m.date_filing.strftime("%Y-%m-%d")}'
                          f' - {m.num_filings} filings')
            else:
                print(f"{ats()} Looks great! No same day filings found.")

    def parse(self):
        """
        Top-level parse procedure.
        :return:
        """

        # Retrieve filings from index db and filter accordingly
        filings = []
        with IndexDb.get_session() as session:
            q = session.query(Filing, Company) \
                .filter(Company.cik == Filing.cik_trust) \
                .order_by(Filing.date_filing, Filing.acc_no)

            # Exit if index is empty
            if q.count() == 0:
                session.close()
                print("Index is empty! Please rebuild.")
                sys.exit(1)

        # Drop filings that should be skipped (e.g. duplicates)
        q = q.filter(Filing.skip == False)

        # Filter by user-defined asset type
        q = q.filter(Company.asset_type.in_(self.asset_types))

        # Apply trust and filing filters
        if len(self.ind_trusts) > 0:
            q = q.filter(Company.cik.in_(self.ind_trusts))
        if len(self.ind_filings) > 0:
            q = q.filter(Filing.acc_no.in_(self.ind_filings))

        # Apply document limit
        if self.n_limit:
            q = q[:self.n_limit]

        if len(q.all()) == 0:
            print(f'{ats()} No filings to parse. Aborting.')
            sys.exit(1)

        # Only leave filings that have not been parsed yet
        if self.rebuild:
            print(f"{ats()} Updating index...")
            for row in q.all():
                row.Filing.is_parsed = False
            print(f'{ats()} Done!')
        else:
            q = q.filter(Filing.is_parsed == False)

        filings = q.all()

        # Some preparation
        if self.use_s3:
            # Use S3
            filings_path = os.path.dirname(__file__)
            s3_resource = boto3.resource('s3')
        else:
            # Use local storage
            filings_path = os.path.join(os.path.dirname(__file__), defaults['filings_folder'])

        # Iterate through entries on the index
        doc_counter = 0
        for row in filings:  # row contains two objects: Filing and Company
            # Build filename
            xml_name = row.Filing.url.split("/")[-1]  # Original filename from filing
            filename = "_".join([row.Filing.date_filing.strftime("%Y-%m-%d"), str(row.Filing.acc_no), xml_name])
            # Get file
            if self.use_s3:
                # Download from s3
                s3_path_components = [row.Company.asset_type, row.Company.name, filename]
                s3_path = "/".join(s3_path_components)
                # Use project folder for temporary storage
                local_path = os.path.join(filings_path, filename)
                try:
                    print(f'{ats()} Downloading filing {s3_path}...')
                    s3_resource.Bucket(defaults['s3_bucket']).download_file(s3_path, local_path)
                except:
                    print(f'{ats()} Could not download filing {s3_path} from s3.')
                    continue
                else:
                    print(f'{ats()} Download complete!')
                    file_path = local_path
            else:
                # Get local file path
                asset_path = os.path.join(filings_path, row.Company.asset_type)
                subfolder_path = os.path.join(asset_path, row.Company.name)
                file_path = os.path.join(subfolder_path, filename)
            print(f'{ats()} Parsing...')

            # Parse xml
            if self.parse_filing(file_path, row.Company.asset_type, row.Filing.acc_no, self.output):
                print(f'{ats()} Parsing complete!')
                print("-" * 5)
            # Remove file from local storage
            os.remove(file_path)
            # Mark filing as parsed in index db
            with IndexDb.get_session() as session:
                f = session.query(Filing).get(row.Filing.acc_no)
                if f is not None:
                    f.is_parsed = True
            # Add filing info to database
            with AssetDb.get_session() as session:
                flng = AssetFiling(
                    accNo=row.Filing.acc_no,
                    trustCik=row.Company.cik,
                    trustName=row.Company.name,
                    url=row.Filing.url,
                    dateFiling=row.Filing.date_filing,
                    assetType=row.Company.asset_type
                )
                session.add(flng)

            doc_counter += 1

        print(f'{ats()} Finished parsing! Parsed {doc_counter} filing(s).')

    @staticmethod
    def parse_filing(file_path, asset_type, acc_no, output):
        """
        Parse individual xml file and save data to database.
        :param file_path: local file path
        :param asset_type: string describing asset type, e.g. autoloan
        :param acc_no: unique filing's number
        :param output: string argument specifying output: csv or db
        :return: True if successful
        """
        with open(file_path, 'rb') as datafile:
            # Preview file and extract namespace
            head = datafile.read(1024).decode('utf-8')
            ns = re.search(r'xmlns="(.*)">', head).group(1)
            nstag = ''.join(['{', ns, '}assetData'])
            datafile.seek(0)
            # Open database session
            with AssetDb.get_session() as session:
                counter = 0
                # Parse the tree
                for event, element in etree.iterparse(datafile, events=('end',), tag=nstag):
                    for assettag in element:
                        # Build list of tuples with fieldname-fieldvalue pairs
                        fields = [(etree.QName(item.tag).localname, item.text) for item in assettag]
                        # Initiate object
                        asset = None
                        if asset_type == 'autoloan':
                            asset = Autoloan()
                        elif asset_type == 'autolease':
                            asset = Autolease()
                        asset.filingAccNo = acc_no
                        # Populate object with properties
                        for field in fields:
                            field_name = field[0]
                            field_value = field[1]
                            # Transform 'special' fields
                            if field_name in asset.special_fields:
                                if asset.special_fields[field_name] == 'Date1':
                                    # Convert date in MM-DD-YYYY format to Date object
                                    dt_arr = field_value.split("-")
                                    field_value = date(int(dt_arr[2]), int(dt_arr[0]),int(dt_arr[1]))
                                elif asset.special_fields[field_name] == 'Date2':
                                    # Convert date in MM/YYYY format to Date object
                                    dt_arr = field_value.split("/")
                                    field_value = date(int(dt_arr[1]), int(dt_arr[0]), 15)
                                elif asset.special_fields[field_name] == 'Boolean':
                                    # Convert 'true' and 'false' strings to True and False python objects
                                    field_value = True if field_value == 'true' else False
                                elif asset.special_fields[field_name] == 'Unlimited':
                                    # Join fields with same tag into strings like '2|3|1'
                                    same_fields = list(filter(lambda x: x[0] == field_name, fields))
                                    if len(same_fields) > 1:
                                        field_value = "|".join([str(f[1]) for f in same_fields])
                                    else:
                                        field_value = str(field_value)
                            # Assign field values to asset object properties
                            setattr(asset, field_name, field_value)
                            # asset_props = asset.__table__.columns._data.keys()
                            # asset_dict = OrderedDict([(prop, getattr(asset, prop)) for prop in asset_props])
                        # Save filing object in db
                        session.add(asset)
                        counter += 1
                        print(f'Processed {counter} records...', end="\r")
        print("")
        return True


def main():

    ap = argparse.ArgumentParser(description="Web scraper for ABS-EE filings.")

    ap.add_argument("-w", "--warn", required=False, action='store_true', default=False,
                    help="issue warnings about same-day filings")
    ap.add_argument("-s", "--s3", required=False, action='store_true', default=False,
                    help="use s3 bucket to collect filings. Run 'aws configure' before using this option.")
    ap.add_argument("-r", "--rebuild", required=False, action='store_true', default=False,
                    help="reparse filings from scratch")
    ap.add_argument("-n", "--number", required=False, type=int, default=0,
                    help="number of filings to parse")
    ap.add_argument("-a", "--asset-type", required=False, type=str, default='autoloan:autolease',
                    help="asset types for downloading separated by ':'")
    ap.add_argument("-t", "--trust", required=False, type=str,
                    help="trust ciks separated by ':'")
    ap.add_argument("-f", "--filing", required=False, type=str,
                    help="filing accession numbers separated by ':'")
    ap.add_argument("-o", "--output", required=False, type=str, default='db',
                    help="output type: csv (default) or db")

    args = vars(ap.parse_args())

    # Check asset type argument
    asset_types = set(args['asset_type'].split(':'))
    if not len(asset_types & {'autoloan', 'autolease', 'rmbs', 'cmbs', 'debtsecurities'}):
        print("Asset types can be autoloan:autolease:rmbs:cmbs:debtsecurities.")
        ap.print_help()
        sys.exit(2)
    # Check if both trust and filing arguments were passed
    if args['trust'] is not None and args['filing'] is not None:
        print("Cannot use both trust and filing options.")
        ap.print_help()
        sys.exit(2)
    # Check output arguments
    if args['output'] == 'csv':
        print('Output to csv is not implemented yet. Please choose output to database instead.')
        ap.print_help()
        sys.exit(2)
    elif args['output'] == 'db':
        pass
    elif args['output'] == 'database':
        args['output'] = 'db'
    else:
        print('Unknown output type:', args['output'])
        ap.print_help()
        sys.exit(2)

    ind_trusts = []
    if args['trust'] is not None:
        ind_trusts = list(map(lambda x: int(x), args['trust'].split(":")))
    ind_filings = []
    if args['filing'] is not None:
        ind_filings = list(map(lambda x: int(x), args['filing'].split(":")))

    # Initiate and run parser
    abs_parser = AbsParser(args['warn'], args['rebuild'], args['s3'], args['number'], asset_types, \
                           ind_trusts, ind_filings, args['output'])
    abs_parser.dispatch()


if __name__ == '__main__':
    main()
