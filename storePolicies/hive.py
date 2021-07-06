from dataclasses import dataclass
import logging

import jaydebeapi
from tabulate import tabulate

HIVE_DRIVER_CLASSPATH = "org.apache.hive.jdbc.HiveDriver"
DEFAULT_LOGGING_FORMAT = '%(asctime)s :: %(levelname)s :: %(module)s :: %(message)s'


@dataclass
class QueryResult:
    columns: list
    rows: list

    def __table__(self):
        return tabulate(self.rows, headers=self.columns, tablefmt='orgtbl')

    def __flat__(self):
        return '\n'.join([x[0] for x in self.rows])


def build_results(raw_columns, raw_rows):
    columns = []
    rows = []
    for col in raw_columns:
        columns.append(str(col[0]))
    if len(raw_rows) > 0:
        for row in raw_rows:
            rows.append(list(row))
    return QueryResult(columns=columns, rows=rows)


@dataclass
class HiveHelperOptions:
    host: str
    port: str
    user: str
    password: str
    hive_jar: str
    schema: str


class HiveHelper:
    def __init__(self, database: str, opts: HiveHelperOptions = None, logger=None):
        self._host = opts.host
        self._port = opts.port
        self._user = opts.user
        self._password = opts.password
        self._hive_jar = opts.hive_jar
        self._database = database
        self._schema = opts.schema

        self._conn: jaydebeapi.Connection = None
        self._curs: jaydebeapi.Cursor = None
        self._active = False

        if not logger:
            logging.basicConfig(level=logging.INFO, format=DEFAULT_LOGGING_FORMAT)
            self.logger = logging.getLogger()
        else:
            self.logger = logger

    def _build_connection(self):
        return {
            'jclassname': HIVE_DRIVER_CLASSPATH,
            'url': self._build_url(),
            'driver_args': {'user': self._user, 'password': self._password},
            'jars': self._hive_jar,
        }

    def _build_url(self):
        return f"jdbc:hive2://{self._host}:{self._port}/{self._database};{self._schema}"

    def set_database(self, database):
        old_database = self._database
        self._database = database
        if self._active:
            self.disconnect()
        self.connect()
        self.logger.info(f"Changed active database from [{old_database}] to"
                         f" [{self._database}]")

    def disconnect(self):
        if not self._active:
            return
        if self._curs:
            self._curs.close()
        if self._conn:
            self._conn.close()
        self._active = False
        self.logger.info(f"Disconnected from database [{self._database}]")

    def connect(self):
        self._conn = jaydebeapi.connect(**self._build_connection())
        self._curs = self._conn.cursor()
        self._active = True
        self.logger.info(f"Connected to database [{self._database}]")

    def run_query(self, sql):
        self._curs.execute(sql)
        raw_results = self._curs.fetchall()
        return build_results(self._curs.description, raw_results)

    def run_statement(self, sql):
        self._curs.execute(sql)
        return f"{self._curs.rowcount} rows affected"

    def get_partitions(self, table_name):
        partition_names = []
        if not self._active:
            raise jaydebeapi.OperationalError("HiveHelper has no active connection(s)")

        partitions_results = self.run_query(f"show partitions {table_name}")
        raw_partitions = [x[0] for x in partitions_results.rows]
        if not raw_partitions:
            return []
        parts = raw_partitions[0].split("/")
        for part in parts:
            part_name, part_val = part.split("=")
            partition_names.append(part_name)
        return partition_names

    def get_tables(self, filter=None):
        if not self._active:
            raise jaydebeapi.OperationalError("HiveHelper has no active connection(s)")

        partitions_results = self.run_query("show tables")
        if filter:
            tables = [x[0] for x in partitions_results.rows if x[0].find(filter) != -1]
        else:
            tables = [x[0] for x in partitions_results.rows]
        if not tables:
            return []
        return tables
