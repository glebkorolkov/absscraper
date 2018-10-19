import argparse
import sys
from helpers import ats, ok


class AbsParser(object):

    def __init__(self, warn=False, use_s3=False, rebuild=False, n_limit=0,
                 asset_types={'autoloan', 'autolease', 'rmbs'}):
        self.warn = warn
        self.use_s3 = use_s3
        self.rebuild = rebuild
        self.n_limit = n_limit
        self.asset_types = asset_types

    def dispatch(self):

        if self.warn:
            self.issue_warnings()
            print(f'{ats()} Done!')
            sys.exit(1)

        if self.rebuild:
            answer = input("You sure you want to reparse? [yes/No]? ")
            if answer.lower() == 'yes':
                pass
                print(f"{ats()} Index cleared...")
            else:
                print(f"{ats()} Aborting...")
                sys.exit(1)

    def issue_warnings(self):
        pass


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
    ap.add_argument("-a", "--asset-type", required=False, type=str, default='autoloan:autolease:rmbs',
                    help="asset types for downloading separated by ':'")

    args = vars(ap.parse_args())

    asset_types = set(args['asset_type'].split(':'))
    if not len(asset_types & {'autoloan', 'autolease', 'rmbs', 'cmbs', 'debtsecurities'}):
        print("Asset types can be autoloan:autolease:rmbs:cmbs:debtsecurities.")
        ap.print_help()
        sys.exit(2)

    # Initiate and run scraper
    abs_parser = AbsParser(args['warn'], args['rebuild'], args['s3'], args['number'], asset_types)
    abs_parser.dispatch()


if __name__ == '__main__':
    main()
