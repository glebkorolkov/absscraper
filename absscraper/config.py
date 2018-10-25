defaults = {

    # Name of subfolder for filing files
    'filings_folder': 'filings',

    # Name of last scraped and saved web page
    'lastpage_filename': 'lastpage.html',

    # Default start search date
    'start_date': '01/01/2016',

    # Default end search date
    'end_date': '12/31/2028',

    # S3 bucket name
    's3_bucket': 'abseeexhibitstorage',

    # Database name
    'db_name': 'index.db'
}

# Credentials for database with asset data
db_config = {
    'db_type': 'mysql',
    'db_user': 'root',
    'db_password': 'root',
    'db_host': '127.0.0.1',
    'db_port': '3306',
    'db_name': 'assets'
}

# db_config = {
#     'db_type': 'mysql',
#     'db_user': 'scraper',
#     'db_password': 'hae6eiN9ke',
#     'db_host': 'ec2-52-91-34-36.compute-1.amazonaws.com',
#     'db_port': '3306',
#     'db_name': 'assets'
# }