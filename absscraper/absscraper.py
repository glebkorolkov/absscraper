import argparse
import sys
import os
import shutil
import re
# import pdb
import requests
import bs4
from datetime import date
from collections import OrderedDict
from config import defaults
from csvfile import CsvFile
from downloader import FileDownloader
from models import Filing, Company
import boto3


class AbsScraper(object):

    """
    Web scraper class to scrape ABS-EE form data from SEC website.
    """

    domain_name = 'https://searchwww.sec.gov'
    url_str = '{domain}/EDGARFSClient/jsp/EDGAR_MainAccess.jsp?search_text=*&sort=ReverseDate' \
              '&formType=FormABSEE&isAdv=true&stemming=false&numResults=100&querySic=6189&fromDate={start}' \
              '&toDate={end}&numResults=100'

    def __init__(self, index=False, download=False, update=False, use_s3=False, n_limit = 0,
                 start_date=defaults['start_date'], end_date=defaults['end_date']):
        # Set run mode defaults
        self.update = update
        self.index = index
        self.download = download
        self.start_date = start_date
        self.use_s3 = use_s3
        self.n_limit = n_limit
        # Build url
        self.start_url = self.url_str.format(domain=self.domain_name, start=start_date, end=end_date)
        # Create assets folder if not exists
        if not os.path.exists(os.path.join(os.path.dirname(__file__), defaults['assets_folder'])):
            os.mkdir(os.path.join(os.path.dirname(__file__), defaults['assets_folder']))
        # Define paths for saved files
        self.lastpage_path = os.path.join(os.path.dirname(__file__),\
                                          defaults['assets_folder'], defaults['lastpage_filename'])
        self.index_path = os.path.join(os.path.dirname(__file__), defaults['assets_folder'], defaults['index_filename'])

    def dispatch(self):

        """
        Main class method that calls other methods depending on mode (index, download, update).
        :return: None
        """
        # Debugging code
        # url = "https://www.sec.gov/Archives/edgar/data/1129987/000112998717000004/fcaot2017-aabsxeejanuary20.htm"
        # url = "https://www.sec.gov/Archives/edgar/data/1689111/000153949716004112/n799_x1-absee.htm"
        # page = self.load_page(url)
        # print(self.parse_absee(page))

        # page = self.load_page(self.start_url)
        # self.scrape_page(page)
        # print(self.scrape_page(page))
        # sys.exit(1)

        if self.index:
            self.build_index()
        if self.download:
            if self.use_s3:
                self.download_filings_s3()
            else:
                self.download_filings()

        print("Finished!")

    def build_index(self):

        """
        Iterates through search result pages and scrapes them for filings information.
        :return: None
        """

        url = self.start_url
        updating = False

        if self.update:
            prev_date = self.update_prepare()
            url = self.url_str.format(domain=self.domain_name, start=prev_date, end=defaults['end_date'])
            updating = True

        all_filings = []
        recorder = CsvFile(filepath=self.index_path)
        page_counter = 0
        entries_counter = 0

        print("Starting index build..." if not updating else "Starting index update...")
        # Iterate through search results pages until no Next button found
        while True:
            page = self.load_page(url)
            filings = self.scrape_page(page)
            # Write filings data from current search results page to csv
            recorder.write_dict(filings, overwrite=(True if (not updating and page_counter == 0) else False))
            page_counter += 1
            entries_counter += len(filings)
            print("Scraped results page {}, {} entries...".format(page_counter, entries_counter))
            all_filings += filings
            # Get url of next search results page
            url = self.get_next(page)
            if url is None:
                # Exit loop if no more search results
                break
            if self.n_limit and entries_counter >= self.n_limit:
                # Exit if reached user-specified limit
                break

        # Do some reporting
        if not updating:
            print("Index built! Total {} search result pages scraped. {} index entries created."
                  .format(page_counter, entries_counter))
        else:
            print("Index updated! Total {} search result page(s) scraped. {} index entries (re)added."
                  .format(page_counter, entries_counter))

        # pdb.set_trace()

    def scrape_page(self, page=None, saved_page=False):

        """
        Scrapes html for filings data.
        :param page: html
        :param saved_page: boolean indicating whether to use a saved web page (for debugging)
        :return: dict with filings data (date, trust name, exhibit type, filing no., cik, filing url)
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
            print('Something\'s wrong. No search results have been found!')
            sys.exit(1)

        filings = []

        # Iterate through rows of the search results table
        for tr in tables[0].select("tr"):
            # Step1. Identify top row of each search result (skip rows with classes blue and infoBorder).
            if 'class' in tr.attrs and any(css_class in tr.attrs['class'] for css_class in ['infoBorder', 'blue']):
                continue

            filing = OrderedDict()

            tds = tr.select("td")
            # Transform filing date in "mm/dd/yyyy" format to date object
            date_str = tds[0].text
            date_arr = date_str.split("/")
            filing_date = date(year=int(date_arr[2]), month=int(date_arr[0]), day=int(date_arr[1]))
            title_links = tds[1].select('a')
            if len(title_links) > 1:
                filing_title = title_links[0].text
                filing_type = filing_title[:6].strip()  # should be either EX-102 or EX-103
                filing_company = filing_title[21:].strip()  # company name as appears in the headline
                filing_url = title_links[1].attrs['href'].strip()
                filing_cik = filing_url.split("/")[6].strip() # headline company cik
                filing_no = filing_url.split("/")[7].strip()

                # Step 2. Check middle row for company names if word "trust" not in headline
                if 'trust' not in filing_company.lower():
                    middle_tr = tr.find_next_sibling("tr", attrs={'class', 'blue'})
                    company_strings = middle_tr.select(".normalbold")
                    if len(company_strings) > 1 and 'trust' in company_strings[1].text.lower():
                        # Override company name if word trust is found in middle row
                        filing_company = company_strings[1].text.split("(")[0].strip()
                        possible_ciks = re.findall(r'\d{7}', company_strings[1].text)
                        if len(possible_ciks) > 0:
                            filing_cik = possible_ciks[0]
                    else:
                        # Step 3. Get lower row with parent filing (abs) url
                        parent_filing_href = tr.find_next_sibling("tr", attrs={'class', 'infoBorder'})\
                            .select("td.footer a.clsBlueBg")[0].attrs['href']
                        abs_url = parent_filing_href[parent_filing_href.find("(")+1:parent_filing_href.find(")")]\
                            .split(",")[0].strip("'")
                        absee_page = self.load_page(abs_url)
                        (abs_cik, abs_trust) = self.parse_absee(absee_page)
                        if abs_cik is not None:
                            filing_cik = abs_cik
                        if abs_trust is not None:
                            filing_company = abs_trust

                filing['date'] = filing_date
                filing['exhibit'] = filing_type
                filing['asset'] = ''
                filing['trust'] = " ".join(filing_company.split("-"))
                filing['cik'] = filing_cik
                filing['no'] = filing_no
                filing['url'] = filing_url

                filings.append(filing)

        return filings

    def parse_absee(self, page):

        """
        Parses ABS-EE filing page attempting to extract issuing company's cik and name
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
        Scrapes html page for Next search results page url
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

    def load_page(self, url):

        """
        Loads html content of given url.
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

        # Save page to a file (for debugging)
        # with open(self.lastpage_path, 'w') as output_file:
        #     output_file.write(content)

        return content

    def update_prepare(self):

        """
        Searches existing index for last entry date and returns it as string.
        Then removes entries with that date from the index
        so that filings as of last available date get reindexed.
        This is done in case SEC uploaded additional filings for the day after previous scraping.
        :return: date string in mm/dd/yyyy format
        """

        # Open existing index file
        filings = self.retrieve_index()

        prev_date = filings[-1]['date'] # get last available filing date (yyyy-mm-dd format)
        # Reformat date to SEC format
        date_arr = prev_date.split("-")
        formatted_date = "{mm}/{dd}/{yyyy}".format(mm=date_arr[1], dd=date_arr[2], yyyy=date_arr[0])

        # Move entries prior to last available date to another dict
        filings_trunc = []
        skipped_counter = 0
        for filing in filings:
            if not filing['date'] == prev_date:
                filings_trunc.append(filing)
            else:
                skipped_counter += 1

        csv_file = CsvFile(self.index_path)
        csv_file.write_dict(filings_trunc, overwrite=True)

        print("Index prepared! {} entries truncated.".format(skipped_counter))
        # Return date string in "mm/dd/yyyy" format
        return formatted_date

    def retrieve_index(self):

        """
        Retrieves filings from saved csv index
        :return: dict of OrderedDicts with filings data
        """

        try:
            csv_file = CsvFile(self.index_path)
            filings = csv_file.read_dict()
        except FileNotFoundError:
            print("Cannot retrieve current index file. Please create index first.")
            sys.exit(1)

        # Abort if index is empty
        if len(filings) < 2:
            print("Index appears to be empty. Please rebuild from scratch.")
            sys.exit(1)

        return filings

    def download_filings(self):

        """
        Creates folder structure for filings to be downloaded based on their csv index
        and launches download routine
        :return: None
        """

        filings = self.retrieve_index()

        if self.n_limit:
            filings = filings[:self.n_limit]

        filings_path = os.path.join(os.path.dirname(__file__), defaults['assets_folder'], defaults['filings_folder'])

        # Remove folder if downloading from scratch
        if os.path.exists(filings_path) and not self.update:
            shutil.rmtree(filings_path)
        # Create folder if not exists
        if not os.path.exists(filings_path):
            os.mkdir(filings_path)
            # Add subfolders for each filing type
            os.mkdir(os.path.join(filings_path, "EX-102"))
            os.mkdir(os.path.join(filings_path, "EX-103"))
        # Iterate through entries on the index
        doc_counter = 0
        for filing in filings:
            # Subfolders are named after trust names
            if filing['exhibit'] == "EX-103":
                subfolder_path = os.path.join(filings_path, filing['exhibit'], filing['trust'])
            else:
                preview = FileDownloader.preview_download(filing['url'])
                asset_type = 'other'
                match = re.search(r'absee/(\w+)/assetdata', preview)
                if match:
                    asset_type = match.group(1)
                asset_path = os.path.join(filings_path, filing['exhibit'], asset_type)
                if not os.path.exists(asset_path):
                    os.mkdir(asset_path)
                subfolder_path = os.path.join(asset_path, filing['trust'])

            # Create subfolder if not exists
            if not os.path.exists(subfolder_path):
                os.mkdir(subfolder_path)
            # Build filename
            filename_arr = ["".join(filing['date'].split("-")),
                            filing['trust'], filing['exhibit'].replace("-", ""), filing['no'], filing['cik']]
            filename_str = ".".join(["-".join(filename_arr), 'xml'])
            filing_path = os.path.join(subfolder_path, filename_str)
            outcome = FileDownloader.download(filing['url'], filing_path)

            if outcome:
                doc_counter += 1

        print("Finished. Downloaded {} documents".format(doc_counter))

    def download_filings_s3(self):

        """
        Downloads filings to s3 storage.
        Downloads a filing to local folder then uploads it to s3 and deletes the local version.
        Run "aws configure" before using this option
        :return: None
        """

        filings = self.retrieve_index()

        if self.n_limit:
            filings = filings[:self.n_limit]

        # Path to temporary save the local version
        temp_path = os.path.join(os.path.dirname(__file__), defaults['assets_folder'])

        s3_client = boto3.client('s3')
        bucket_name = defaults['s3_bucket']
        s3_resource = boto3.resource('s3')
        # Delete all folders in the bucket
        if not self.update:
            bucket_obj = s3_resource.Bucket(bucket_name)
            bucket_obj.objects.all().delete()

        doc_counter = 0
        for filing in filings:

            # Build filename and temporary file path
            filename_arr = ["".join(filing['date'].split("-")),
                            filing['trust'], filing['exhibit'].replace("-", ""), filing['no'], filing['cik']]
            filename_str = ".".join(["-".join(filename_arr), 'xml'])
            filing_path = os.path.join(temp_path, filename_str)

            # Determine asset type and build s3 path
            if filing['exhibit'] == "EX-103":
                s3_path_components = [filing['exhibit'], filing['trust'], filename_str]
            else:
                preview = FileDownloader.preview_download(filing['url'])
                asset_type = 'other'
                match = re.search(r'absee/(\w+)/assetdata', preview)
                if match:
                    asset_type = match.group(1)
                s3_path_components = [filing['exhibit'], asset_type, filing['trust'], filename_str]
            s3_path = "/".join(s3_path_components)

            try:
                # Check if file exists on s3
                s3_resource.Object(bucket_name, s3_path).load()
            except:
                outcome = FileDownloader.download(filing['url'], filing_path)
                if not outcome:
                    print("Could not download url: {}".format(filing['url']))
                else:
                    doc_counter += 1
                    s3_client.upload_file(filing_path, bucket_name, s3_path)
                    print("Filing {} out of {} uploaded successfully. Filename: {}"
                          .format(doc_counter, len(filings), filename_str))
                    os.remove(filing_path)

        # Upload csv index file
        s3_client.upload_file(self.index_path, bucket_name, 'index.csv')
        print("Index uploaded.")
        print("Finished. Downloaded {} documents".format(doc_counter))


def main(argv):

    ap = argparse.ArgumentParser(description="Web scraper for ABS-EE filings.")

    ap.add_argument("-i", "--index", required=False, action='store_true', default=False,
                    help="build index")
    ap.add_argument("-d", "--download", required=False, action='store_true', default=False,
                    help="download filings")
    ap.add_argument("-s", "--s3", required=False, action='store_true', default=False,
                    help="use s3 bucket for storage. Run 'aws configure' before using this option.")
    ap.add_argument("-u", "--update", required=False, action='store_true', default=False,
                    help="build index / download filings starting from latest available")
    ap.add_argument("-n", "--number", required=False, type=int, default=0,
                    help="number of filings to download/index")

    args = vars(ap.parse_args())

    if not args['index'] and not args['download']:
        print("Please specify either [-u --update] or [-d --download] option. Other options are optional.")
        ap.print_help()

    # Initiate and run scraper
    scraper = AbsScraper(args['index'], args['download'], args['update'], args['s3'], args['number'])
    scraper.dispatch()


if __name__ == '__main__':
    main(sys.argv[1:])
