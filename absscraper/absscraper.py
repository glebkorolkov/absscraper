import argparse
import sys
import os
import shutil
import re
import requests
import bs4
import boto3
from datetime import date
from config import defaults
from helpers import FileDownloader, ats, ok
from models import IndexDb, Filing, Company


class AbsScraper(object):

    """
    Web scraper class to scrape ABS-EE form data from SEC website.
    """

    domain_name = 'https://searchwww.sec.gov'
    url_str = '{domain}/EDGARFSClient/jsp/EDGAR_MainAccess.jsp?search_text=*&sort=ReverseDate' \
              '&formType=FormABSEE&isAdv=true&stemming=false&numResults=100&querySic=6189&fromDate={start}' \
              '&toDate={end}&numResults=100'

    def __init__(self, index=False, download=False, rebuild=False, use_s3=False, n_limit=0,
                 asset_types={'autoloan', 'autolease', 'rmbs'},
                 start_date=defaults['start_date'], end_date=defaults['end_date']):
        # Set run mode defaults
        self.rebuild = rebuild
        self.index = index
        self.download = download
        self.start_date = start_date
        self.use_s3 = use_s3
        self.n_limit = n_limit
        self.asset_types = asset_types
        # Build url
        self.start_url = self.url_str.format(domain=self.domain_name, start=start_date, end=end_date)
        # Define paths for saved html
        self.lastpage_path = os.path.join(os.path.dirname(__file__), defaults['lastpage_filename'])

    def dispatch(self):

        """
        Main class method that calls other methods depending on mode (index, download, update).
        :return: None
        """
        if self.index:
            if self.rebuild:
                answer = input("You sure you want to rebuild? [yes/No]? ")
                if answer.lower() == 'yes':
                    # Drop and recreate tables
                    db = IndexDb()
                    db.clear()
                    db.setup()
                    print(f"{ats()} Index cleared...")
                else:
                    print(f"{ats()} Aborting...")
                    sys.exit(1)
            self.build_index()

        if self.download:
            self.download_filings()

        print(f"{ats()} Finished. Good job!")
        ok()

    def build_index(self):
        """
        Iterate through search result pages and scrape them for filings information.
        :return: None
        """
        url = self.start_url

        # Search from last available date if not rebuilding and index is not empty
        if not self.rebuild > 0:
            recent_filings = self.get_most_recent_filings()
            pdt = recent_filings[0].date_filing
            # Reformat date to SEC format MM/DD/YYYY
            formatted_date = f"{pdt:02}/{pdt:02}/{pdt.year}"
            url = self.url_str.format(domain=self.domain_name, start=formatted_date, end=defaults['end_date'])

        page_counter = 0
        entries_counter = 0

        print(f"{ats()} Starting index build..." if self.rebuild else f"{ats()} Starting index update...")
        # Iterate through search results pages until no Next button found
        while True:
            page = self.load_page(url)
            # Scrape, parse and record into database current search results page
            entries_counter += self.scrape_page(page)
            page_counter += 1
            print(f"{ats()} Scraped results page {page_counter}, {entries_counter} entries...")
            # Get url of next search results page
            url = self.get_next(page)
            if url is None:
                # Exit loop if no more search results
                break
            if self.n_limit and entries_counter >= self.n_limit:
                # Exit if reached user-specified limit
                break

        # Do some reporting
        if self.rebuild:
            print(f'{ats()} Index built! Total {page_counter} search result pages scraped. '
                  f'{entries_counter} index entries created.')
        else:
            print(f'{ats()} Index updated! Total {page_counter} search result page(s) scraped. '
                  f'{entries_counter} index entries (re)added.')

    def scrape_page(self, page=None, counter=0, saved_page=False):
        """
        Scrape html for filings data and add them to db.
        :param page: html
        :param counter: technical var to keep track of processed filings
        :param saved_page: boolean indicating whether to use a saved web page (for debugging)
        :return: number of processed filings
        """
        # Retrieved last saved page (if available) for debugging purposes or send a request for starting page
        if saved_page:
            try:
                with open(self.lastpage_path, 'r') as input_file:
                    page = input_file.read()
            except:
                page = self.load_page()

        soup = bs4.BeautifulSoup(page, features="html.parser")
        tables = soup.find_all("table", attrs={'xmlns:autn': "http://schemas.autonomy.com/aci/"})
        if len(tables) == 0:
            print(f'{ats()} Something\'s wrong. No search results have been found!')
            sys.exit(1)

        # Iterate through rows of the search results table
        for tr in tables[0].select("tr"):
            # Identify top row of each search result (skip rows with classes blue and infoBorder).
            if 'class' in tr.attrs and any(css_class in tr.attrs['class'] for css_class in ['infoBorder', 'blue']):
                continue

            tds = tr.select("td")
            # Transform filing date in "mm/dd/yyyy" format to date object
            date_str = tds[0].text
            date_arr = date_str.split("/")
            filing_date = date(year=int(date_arr[2]), month=int(date_arr[0]), day=int(date_arr[1]))

            title_links = tds[1].select('a')
            if len(title_links) > 1:
                filing_title = title_links[0].text
                filing_type = filing_title[:6].strip()  # should be either EX-102 or EX-103
                # Skip EX-103 exhibits
                if filing_type == 'EX-103':
                    continue

                filing_url = title_links[1].attrs['href'].strip()
                acc_no = filing_url.split("/")[7].strip()
                filer_cik = filing_url.split("/")[6].strip()  # headline company cik
                filer_company = filing_title[21:].strip()  # company name as appears in the headline
                trust_company = filer_company
                trust_cik = filer_cik
                # Get more information on company
                middle_tr = tr.find_next_sibling("tr", attrs={'class', 'blue'})
                company_strings = middle_tr.select(".normalbold")

                if len(company_strings) > 1:
                    # If two company names below headline
                    a1_company = company_strings[0].text.split("(")[0].strip()
                    a2_company = company_strings[1].text.split("(")[0].strip()
                    # Cik can be 6 or 7 digits
                    a1_cik = re.findall(r'\d{6,8}', company_strings[0].text)[0]
                    a2_cik = re.findall(r'\d{6,8}', company_strings[1].text)[0]
                    # Trust's cik is always greater than filer's
                    if int(a1_cik) > int(a2_cik):
                        trust_company = a1_company
                        trust_cik = a1_cik
                    else:
                        trust_company = a2_company
                        trust_cik = a2_cik
                else:
                    # If only one company's name below headline, dig deeper
                    parent_filing_href = tr.find_next_sibling("tr", attrs={'class', 'infoBorder'})\
                        .select("td.footer a.clsBlueBg")[0].attrs['href']
                    abs_url = parent_filing_href[parent_filing_href.find("(")+1:parent_filing_href.find(")")]\
                        .split(",")[0].strip("'")
                    absee_page = self.load_page(abs_url)
                    (abs_cik, abs_trust) = self.parse_absee(absee_page)
                    if abs_cik is not None:
                        trust_cik = abs_cik
                    if abs_trust is not None:
                        trust_company = abs_trust

                # Save company and filing data into db
                with IndexDb.get_session() as session:
                    asset_type = None
                    # Save trust company if does not exist
                    tco = session.query(Company).get(trust_cik)
                    if tco is None:
                        preview = FileDownloader.preview_download(filing_url)
                        match = re.search(r'absee/(\w+)/assetdata', preview)
                        if match:
                            asset_type = match.group(1)
                        tco = Company(cik=trust_cik, name=trust_company, is_trust=True, asset_type=asset_type)
                        session.add(tco)
                    # Save filer company if does not exist
                    if not filer_cik == trust_cik:
                        fco = session.query(Company).get(filer_cik)
                        if fco is None:
                            fco = Company(cik=filer_cik, name=filer_company, is_trust=False, asset_type=asset_type)
                            session.add(fco)
                    # Save filing data
                    filing = Filing(acc_no=acc_no,
                                    cik_filer=filer_cik,
                                    cik_trust=trust_cik,
                                    url=filing_url,
                                    date_filing=filing_date)
                    # Save only if no database entry found for this accession no
                    if session.query(Filing).get(acc_no) is None:
                        session.add(filing)

                    counter += 1
                    print(f'{ats()} Done with {filer_company}-{filer_cik} from {filing_date}...')

        return counter

    @staticmethod
    def parse_absee(page):
        """
        Parse ABS-EE filing page attempting to extract issuing company's cik and name
        as opposed to depositor's cik and name
        :param page: html of ABS-EE
        :return: (cik, issuer's name). Either can be None if not found.
        """
        cik = None
        trust = None

        soup = bs4.BeautifulSoup(page, features="html.parser")
        # Extract trust's cik
        page_text = soup.get_text(" ", strip=True).replace("\n", " ")
        match = re.search(r'issuing entity: (\d{10})', page_text)
        if match:
            cik = match.group(1)
        # Extract trust's name
        page_text = soup.get_text("|", strip=True).replace("\n", " ")
        text_lines = page_text.split("|")
        for i, line in enumerate(text_lines):
            if '(Exact name of issuing' in line or '(Exact name of the issuing' in line:
                trust = text_lines[i-1]

        return cik, trust

    def get_next(self, page=None):
        """
        Scrape html page for Next search results page url.
        :param page: html
        :return: None
        """
        if page is None:
            return None

        soup = bs4.BeautifulSoup(page, features="html.parser")
        a_next = soup.select("a[title='Next Page']")
        if len(a_next) > 0:
            return self.domain_name + a_next[0].attrs['href']

        return None

    @staticmethod
    def load_page(url):
        """
        Load html content of a given url.
        :param url: url of web page
        :return: html string
        """
        parameters = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/69.0.3497.100 Safari/537.36"}
        response = requests.get(url, params=parameters)

        # Abort if server is responding with error
        if not response.status_code == 200:
            print("Server stopped responding. Execution aborted.")
            sys.exit(1)

        content = response.content.decode(response.encoding)

        # Save page to a file for debugging
        # with open(self.lastpage_path, 'w') as output_file:
        #     output_file.write(content)

        return content

    @staticmethod
    def get_most_recent_filings():
        """
        Look up most recent date's filings in the db.
        :return: list of dicts or empty dict
        """
        with IndexDb.get_session() as session:
            last_filing = session.query(Filing).order_by(Filing.date_filing.desc()).first()
            if last_filing is None:
                return []
            return session.query(Filing)\
                .filter(Filing.date_filing == last_filing.date_filing).all()

    def download_filings(self):
        """
        Create folder structure for filings to be downloaded based on their csv index
        and launch download routine.
        :return: None
        """
        filings = []

        with IndexDb.get_session() as session:
            q = session.query(Filing, Company)\
                .filter(Company.cik == Filing.cik_trust)\
                .order_by(Filing.date_filing, Filing.acc_no)

            # Exit if index is empty
            if q.count() == 0:
                session.close()
                print("Index is empty! Please rebuild.")
                sys.exit(1)

            # Apply document limit
            if self.n_limit:
                q = q[:self.n_limit]

            # Only leave filings that have not been downloaded
            if self.rebuild:
                print(f"{ats()} Updating index...")
                for row in q.all():
                    row.Filing.is_downloaded=False
                print(f'{ats()} Done!')
            else:
                q = q.filter(Filing.id_downloaded == False)

            # Filter by user-defined asset type
            q = q.filter(Company.asset_type.in_(self.asset_types))
            filings = q.all()

        # Prepare storage
        if self.use_s3:
            # Use S3
            filings_path = os.path.dirname(__file__)
            s3_client = boto3.client('s3')
            bucket_name = defaults['s3_bucket']
            s3_resource = boto3.resource('s3')
            # Delete all folders in the bucket
            if self.rebuild:
                bucket_obj = s3_resource.Bucket(bucket_name)
                bucket_obj.objects.all().delete()
        else:
            # Use local storage
            filings_path = os.path.join(os.path.dirname(__file__), defaults['filings_folder'])
            # Remove folder if downloading from scratch
            if os.path.exists(filings_path) and self.rebuild:
                shutil.rmtree(filings_path)
            # Create folder if not exists
            if not os.path.exists(filings_path):
                os.mkdir(filings_path)

        # Iterate through entries on the index
        doc_counter = 0
        for row in filings: # row contains two objects: Filing and Company
            # Build filename
            xml_name = row.Filing.url.split("/")[-1]  # Original filename from filing
            filename = "_".join([row.Filing.date_filing.strftime("%Y-%m-%d"), str(row.Filing.acc_no), xml_name])
            # Build filepath
            if self.use_s3:
                # Use project folder for temporary storage
                subfolder_path = filings_path
            else:
                # Create folder tree
                asset_path = os.path.join(filings_path, row.Company.asset_type)
                if not os.path.exists(asset_path):
                    os.mkdir(asset_path)
                subfolder_path = os.path.join(asset_path, row.Company.name)
                # Create subfolder if not exists
                if not os.path.exists(subfolder_path):
                    os.mkdir(subfolder_path)

            # Download file
            download_path = os.path.join(subfolder_path, filename)
            print("-"*5)
            print(f"{ats()} Downloading document {row.Filing.url} ...")
            failed_counter = 0
            downloaded = False
            try:
                downloaded = FileDownloader.download(row.Filing.url, download_path)
            except:
                print(f"{ats()} Download failed for document {row.Filing.url} Skipping...")
                failed_counter += 1
                if failed_counter == 5:
                    print(f"{ats()} Failed downloading several documents. Aborting...")
                    sys.exit(1)

            if downloaded:
                print(f"{ats()} Downloaded successfully!")
                # Upload to s3
                if self.use_s3 and downloaded:
                    s3_path_components = [row.Company.asset_type, row.Company.name, filename]
                    s3_path = "/".join(s3_path_components)
                    try:
                        # Check if file exists on s3
                        s3_resource.Object(bucket_name, s3_path).load()
                    except:
                        print(f"{ats()} Uploading to s3...")
                        s3_client.upload_file(download_path, bucket_name, s3_path)
                        print(f'{ats()} Uploaded document {s3_path}')
                        os.remove(download_path)
                # Update index
                with IndexDb.get_session() as session:
                    f = session.query(Filing).get(row.Filing.acc_no)
                    if f is not None:
                        f.is_downloaded = True
                        # f.update()
                doc_counter += 1
            else:
                print(f"{ats()} Could not download url: {row.Filing.url}")

        if self.use_s3:
            print(f'{ats()} Finished. Downloaded and uploaded to s3 {doc_counter} documents.')
        else:
            print(f'{ats()} Finished. Downloaded {doc_counter} documents.')


def main():

    ap = argparse.ArgumentParser(description="Web scraper for ABS-EE filings.")

    ap.add_argument("-i", "--index", required=False, action='store_true', default=False,
                    help="build index")
    ap.add_argument("-d", "--download", required=False, action='store_true', default=False,
                    help="download filings")
    ap.add_argument("-s", "--s3", required=False, action='store_true', default=False,
                    help="use s3 bucket for storage. Run 'aws configure' before using this option.")
    ap.add_argument("-r", "--rebuild", required=False, action='store_true', default=False,
                    help="rebuild index / re-download filings from scratch")
    ap.add_argument("-n", "--number", required=False, type=int, default=0,
                    help="number of filings to download/index")
    ap.add_argument("-a", "--asset-type", required=False, type=str, default='autoloan:autolease:rmbs',
                    help="asset types for downloading separated by ':'. Ignored for indexing.")

    args = vars(ap.parse_args())

    if not args['index'] and not args['download']:
        print("Please specify either [-i --index] or [-d --download] option. Other options are optional.")
        ap.print_help()
        sys.exit(2)

    asset_types = set(args['asset_type'].split(':'))
    if not len(asset_types & {'autoloan', 'autolease', 'rmbs', 'cmbs', 'debtsecurities'}):
        print("Asset types can be autoloan:autolease:rmbs:cmbs:debtsecurities.")
        ap.print_help()
        sys.exit(2)

    # Initiate and run scraper
    scraper = AbsScraper(args['index'], args['download'], args['rebuild'], args['s3'], args['number'], asset_types)
    scraper.dispatch()


if __name__ == '__main__':
    main()
