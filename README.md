# Web Scraper for ABS-EE loan data

## Background

This is a web scraper and parser for data contained in ABS-EE filings on the 
[SEC website](https://searchwww.sec.gov/EDGARFSClient/jsp/EDGAR_MainAccess.jsp?search_text=*&sort=Date&formType=FormABSEE&isAdv=true&stemming=true&numResults=10&querySic=6189&fromDate=11/01/2016&toDate=10/10/2018&numResults=10). 
ABS-EE filings captured my interest because they contain asset-level data on loan porfolios underlying 
[asset-backed securities](https://en.wikipedia.org/wiki/Asset-backed_security) 
(ABS). ABS are issued by large lenders such as auto loan companies and mortgage providers 1) 
allowing them free up their balance sheets, and 2) enabling public investors to invest in new 
asset classes.

ABS have been around since 1980's, however, in November 2016 their issuers were obliged to
disclose asset-level data to the public on a monthly basis. Data on hundreds of thousands 
of auto loans, auto leases and commercial mortgages in the US (stripped of any personal 
identifying information) is now freely available online for exploration and analysis.

Data exists in the form of large xml files submitted to the SEC by ABS issuers. The goal of my project 
was to scrape the SEC web site for relevant filings, parse xml data from the filings and save it
to a database for further analysis. Given sheer volume of available data I restricted 
the scope of my project to auto loans and auto leases. 

## Workflow

Workflow includes 3 steps:

1. Web scraping

2. Parsing

3. Pre-processing

## Web Scraping

`absparser.py` is responsible for web scraping. The code searches for relevant filings on the
[SEC EDGAR website](https://www.sec.gov/edgar/searchedgar/companysearch.html) and collects 
urls of filings' addenda that actually contain the data. It then downloads xml files from urls
one by one and saves them either locally or in a specified AWS S3 bucket. Information about
issuers and filings is saved to a Sqlite database.

To be able to save filings to S3 you must create an S3 bucket, configure aws credentials and edit
bucket name in `config.py`.

`absparser.py` should be used as a command line utility that accepts one or few parameters.

For example, to build an index of available filings (list of filings and issuers as well as
filings' urls) type:

```bash
python absscraper.py -i
```
or if you want to erase the index and rebuild it from scratch:
```bash
python absscraper.py -ir
```
After you have built the index you can start downloading the data. To download all data and
save it locally enter:
```bash
python absscraper.py -d
```
or to save xml files to an S3 bucket:
```bash
python absscraper.py -ds
```
If you do not want to download all filings or would like to limit them to one or several
asset classes you may want to use `-n` and `-a` parameters. For example, the below command deletes
all previously downloaded files from S3 and downloads 100 xmls with auto loan and 
auto lease data:
```bash
python absscraper.py -drs -n 100 -a "autoloan:autolease"
```
By default, if you do not specify the `-r` parameter, previously downloaded files are skipped. 
This may come in handy if download was interrupted or if you are running an update.

After completing this step you should have an index of available ABS-EE filings in a local Sqlite database
and a collection of xml files in a local folder or in S3.

Scraping works for all asset classes: auto loans, auto leases, and commercial mortgages.

## Parsing
In the parsing step saved xml files are downloaded from S3 or retrieved from local storage
and parsed. Parsed data gets saved to a MySQL database. You need to have a running MySQL
server and edit `config.py` for this step. Parsing is only supported for auto loan data.

Use `absparser.py` with parameters to parse xml data. Run
```bash
python absparser.py -a autoloan
```
to parse xml files from local `data` folder (or other folder specified in `config.py`). Or
```bash
python absparser.py -as autoloan
```
to parse filings saved in S3.

To limit the number of parsed xmls use `-n` parameter (for instance, `-n 100` will limit 
the number of parsed filings to 100). To parse filings with particular identifiers 
(accession numbers) use `-f` parameter. Or use `-t` to parse filings from one or several
issuers (trusts). The following command will erase all previously parsed data and reparse 
10 filings from the trust with identifier (cik) `123456`: 
```bash
python absparser.py -rs -t 123456 -n 10
```
When you are done with parsing you will have a MySQL database of auto loan data that can be
used for further analysis.

Note: Sometimes issuers file two versions of the same data on the same day. To run checks for same-day filings
enter:
```bash
python absparser.py -w
```
Then inspect same-day filings manually.

## Pre-processing

This step is optional. After parsing you will end up with data that is panel data (multiple data points 
for each loan for multiple periods). Pre-processing transforms panel data to cross-sectional data
by aggregating data points across time periods. For example, the below command will pre-process data for ABS
issued by Toyota trusts. 
```bash
python abshandler.py -rq -c toyota
```
I then use pre-processed data on loans issued by a number of car manufacturers in my other project &ndash; 
[Interactive Auto Loan Dashboard](https://github.com/glebkorolkov/absdashboard).

## Next steps
* Parsing only works for auto loans now. Should be expanded to other asset classes
* Get rid of Sqlite database for indexing
* Implement option to save parsed data to csv rather than MySQL database

## See also
I used auto loan data scraped from the SEC web site to build an interactive 
[Plotly Dash](https://plot.ly/products/dash/) based dashboard available 
[here](https://github.com/glebkorolkov/absdashboard).

I collected auto loan and auto lease data from November 2016 to October 2018 and saved it in
a public S3 bucket here: `s3://abseeexhibitstorage/`.