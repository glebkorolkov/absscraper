import os
import sqlite3
from datetime import datetime
from config import defaults


class SqliteDb(object):

    """
    Generic class for Sqlite3 requests
    """

    def __init__(self, db_name):
        self.db_name = db_name

    def execute(self, query, params=[]):

        """
        Executes single queries for list of queries. For write-operations only.
        :param query: string or list of query strings
        :param params: list of optional parameters if "?" syntax is used
        :return: None
        """

        query_list = []
        if type(query) is str:
            query_list.append(query)
        elif type(query) is list:
            query_list += query

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        for q in query_list:
            c.execute(q, params)
        conn.commit()
        conn.close()

    def query(self, query):

        """
        Executes single read-queries.
        :param query: query string
        :return: list of tuples
        """

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute(query)
        response = c.fetchall()
        conn.close()
        return response


class IndexDb(SqliteDb):

    """
    Class for Sqlite3-based index of scraped filings
    """

    def __init__(self, db_name=None):
        """
        Constructor. Only assigns database name and checks for existing db file.
        :param db_name:
        """
        if db_name is None:
            db_name = defaults['db_name']

        super().__init__(db_name)

        # Prompt user to confirm creation of new db if existing not found
        db_path = os.path.join(os.path.dirname(__file__), self.db_name)
        if not os.path.exists(db_path):
            print("Database does not exist!")
            answer = input("Do you want to create a new one? [yes/No]? ")
            if answer.lower() == 'yes':
                self.setup()
            else:
                raise Exception("Could not create index database.")

    def setup(self):
        """
        Creates database structure (empty tables).
        :return: None
        """
        queries = list()
        queries.append('DROP TABLE IF EXISTS `filings`')
        queries.append('DROP TABLE IF EXISTS `companies`')
        queries.append('CREATE TABLE `filings` ('
                       '`acc_no` INTEGER NOT NULL UNIQUE, '
                       '`cik_filer` INTEGER, `cik_trust` INTEGER, '
                       '`url` TEXT, '
                       '`is_downloaded` INTEGER DEFAULT 0, '
                       '`is_parsed` INTEGER DEFAULT 0, '
                       '`date_filing` TEXT, '
                       '`date_add` TEXT, '
                       '`date_upd` TEXT, '
                       'PRIMARY KEY(`acc_no`) )')
        queries.append('CREATE TABLE `companies` ( '
                       '`cik` INTEGER NOT NULL UNIQUE, '
                       '`name` TEXT, '
                       '`is_trust` INTEGER DEFAULT 0, '
                       '`asset_type` TEXT, '
                       '`date_add` TEXT, '
                       'PRIMARY KEY(`cik`) )')
        self.execute(queries)

    def clear(self):
        """
        Clears all tables.
        :return: None
        """
        queries = list()
        queries.append('DELETE FROM `filings`;')
        queries.append('DELETE FROM `companies`')
        self.execute(queries)

    @staticmethod
    def get_instance():
        """
        Returns an instance of IndexDb object for use by other classes.
        :return: IndexDb class object
        """
        return IndexDb()


class IndexObject(object):

    """
    General class to be extended by more specific classes. Should not be used on it own.
    """

    table_name = None
    table_fields = []

    def __init__(self):
        pass

    def get_db_fields(self):
        """
        Selects object attributes that correspond to db table
        :return: dict of attribute : value pairs
        """
        return {k: vars(self)[k] for k in self.table_fields}

    def add(self):
        """
        INSERTs into db a row matching the object
        :return:
        """
        query_parts = list()
        query_parts.append(f'INSERT INTO {self.table_name} (')
        query_parts.append(', '.join(self.get_db_fields().keys()))
        query_parts.append(') VALUES (')
        query_parts.append(', '.join('?' for v in self.get_db_fields().values()))
        query_parts.append(');')
        db = IndexDb.get_instance()
        db.execute("".join(query_parts), list(self.get_db_fields().values()))

    def save(self):
        """
        Alias for update()
        :return:
        """
        self.update()

    def update(self):
        """
        Updates existing db record with object attributes by calling UPDATE.
        :return:
        """
        fields = list(self.get_db_fields().keys())[1:]
        values = list(self.get_db_fields().values())[1:]
        pk = list(self.get_db_fields().keys())[0]
        query_parts = list()
        query_parts.append(f'UPDATE {self.table_name} SET ')
        query_parts.append(', '.join([f'{k}=?' for k in fields]))
        query_parts.append(' WHERE ')
        query_parts.append(pk)
        query_parts.append("=")
        query_parts.append(str(getattr(self, pk)))
        query_parts.append(';')
        db = IndexDb.get_instance()
        db.execute("".join(query_parts), values)

    def delete(self):
        """
        DELETEs a record matching the object from the db.
        :return:
        """
        pk = self.table_fields[0]
        if getattr(self, pk) is not None:
            query = f'DELETE FROM {self.table_name} WHERE {pk}=?;'
            db = IndexDb.get_instance()
            db.execute(query, [getattr(self, pk)])
            del self

    def get_row(self):
        """
        Returns db row with object attributes.
        :return: tuple with attributes or None if pk was not found in the database
        """
        query = f'SELECT * FROM {self.table_name} WHERE {self.table_fields[0]}={getattr(self, self.table_fields[0])};'
        rows = IndexDb.get_instance().query(query)
        if len(rows) == 0:
            return None
        return {f: p for f, p in zip(self.table_fields, rows[0])}

    def get_obj(self):
        """
        Populates object attributes with data from the db.
        :return: True if entry was found in db, False otherwise
        """
        row = self.get_row()
        if row is None:
            return False
        # Populate object attributes with values from db
        for k, val in row.items():
            setattr(self, k, val)
        return True

    @classmethod
    def get_all_rows(cls):
        """
        Returns all rows for objects in db.
        :return: list of dicts or None
        """
        query = f'SELECT * FROM {cls.table_name} WHERE 1;'
        rows = IndexDb.get_instance().query(query)
        if len(rows) == 0:
            return None
        return [{f: p for f, p in zip(cls.table_fields, row)} for row in rows]

    @classmethod
    def get_filtered_rows(cls, filters={}):
        """
        Returns rows for objects in db filtered by a set of 'equal' conditions.
        :return: list of dicts or None
        """
        fstr = "1"
        if len(filters):
            fstr = ' AND '.join([k + '=' + str(v*1) for k, v in filters.items()])

        query_parts = list()
        query_parts.append(f'SELECT * FROM {cls.table_name} WHERE ')
        query_parts.append(fstr)
        query_parts.append(';')
        print(''.join(query_parts))
        rows = IndexDb.get_instance().query(''.join(query_parts))
        if len(rows) == 0:
            return None
        return [{f: p for f, p in zip(cls.table_fields, row)} for row in rows]


class Filing(IndexObject):

    """
    Class representing ABS-EE filing
    """

    table_name = 'filings'
    table_fields = [
        'acc_no',
        'cik_filer',
        'cik_trust',
        'url',
        'is_downloaded',
        'is_parsed',
        'date_filing',
        'date_add',
        'date_upd'
    ]

    def __init__(self, acc_no, cik_filer=None, cik_trust=None, url=None, is_downloaded=False, is_parsed=False,
                 date_filing=None, date_add=None, date_upd=None):
        super().__init__()
        self.acc_no = acc_no
        self.cik_filer = cik_filer
        self.cik_trust = cik_trust
        self.url = url
        self.is_downloaded = is_downloaded
        self.is_parsed = is_parsed
        self.date_filing = date_filing
        self.date_add = date_add
        self.date_upd = date_upd

    def __str__(self):
        return f'Filing\n' \
               f'acc_no: {self.acc_no}\n' \
               f'url: {self.url}\n' \
               f'cik_trust: {self.cik_trust}'

    def add(self):
        """
        Overrides parent class by automatically adding insert time
        :return: None
        """
        self.date_add = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.date_upd = self.date_add
        super().add()

    def update(self):
        """
        Overrides parent class by automatically modifying update time
        :return: None
        """
        self.date_upd = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        super().update()

    @staticmethod
    def get_obj_by_acc_no(acc_no):
        """
        Returns a filing object from db based on accession number
        :param acc_no: filing accession number (integer)
        :return: Filing object or None
        """
        obj = Filing(acc_no)
        if obj.get_obj():
            return obj
        return None


class Company(IndexObject):

    """
    Class representing company (filer or trust) associated with filings
    """

    table_name = 'companies'
    table_fields = [
                'cik',
                'name',
                'is_trust',
                'asset_type',
                'date_add'
            ]

    def __init__(self, cik, name=None, is_trust=False, asset_type=None, date_add=None):
        super().__init__()
        self.cik = cik
        self.name = name
        self.is_trust = is_trust
        self.asset_type = asset_type
        self.date_add = date_add

    def __str__(self):
        return f'Company\n' \
               f'cik: {self.cik}\n' \
               f'name: {self.name}\n' \
               f'asset_type: {self.asset_type}\n' \
               f'is_trust: {self.is_trust}'

    def add(self):
        """
        Overrides parent class by automatically adding insert time
        :return: None
        """
        self.date_add = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        super().add()

    @staticmethod
    def get_obj_by_cik(cik):
        """
        Returns a company object from db based on company's central index key (cik) number.
        :param cik: central index key (integer)
        :return: Company object or None
        """
        obj = Company(cik)
        if obj.get_obj():
            return obj
        return None
