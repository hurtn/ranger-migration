import json
import logging
import pathlib

import jaydebeapi
from security.keyvault import get_ms_credentials


# Class for storing the information about the Hive DB storage accounts
class HiveDBMetadata:

    def __init__(self, name, desc, path, owner_name, owner_type, parameters, ranger_policy_id):
        self._name = name
        self._desc = desc
        self._path = path
        self._owner_name = owner_name
        self._owner_type = owner_type
        self._parameters = parameters
        self._ranger_policy_id = ranger_policy_id

    @property
    def name(self):
        return self._name

    @property
    def desc(self):
        return self._desc

    @property
    def path(self):
        return self._path

    @property
    def owner_name(self):
        return self._owner_name

    @property
    def owner_type(self):
        return self._owner_type

    @property
    def parameters(self):
        return self._parameters

    @property
    def ranger_policy_id(self):
        return self._ranger_policy_id


# Gets the Hive metastore configurations set in the conf/metastore_conf.json file.
def get_ms_conf():
    data = None
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = proj_home_abs_path + "conf/metastore_conf.json"
    with open('conf_file_path') as json_file:
        data = json.load(json_file)
    logging.info("Read the Hive MS config")

    return data


# Connect to the Hive instance
def connect_hive(server, port, database, ms_key, user_name):
    # Construct the JDBC connection string
    url = ("jdbc:hive2://" + server + ":" + str(port) + "/" + database +
           ";transportMode=http;ssl=true;httpPath=/hive2")

    # Connect to Hive
    password = "Qwe12rty!!"
    # password = get_ms_credentials(ms_key)
    conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, {'user': user_name,
                                                                       'password': password})
    return conn.cursor()


# Fetches all the Hive DBs from the metastore, puts the same into the HiveDBMetadata object
def fetch_hive_dbs():
    logging.info("fetch_hive_dbs() start")

    # Read in the configurations for metastore
    logging.info("Reading the hive ms config")
    ms_conf_dict = get_ms_conf()
    logging.debug(ms_conf_dict)

    # Use the metastore configs to connect to HDInsight Hive & get the cursor
    logging.info("Connecting to hive ms")
    cursor = connect_hive(ms_conf_dict.server, ms_conf_dict.port, ms_conf_dict.database, ms_conf_dict.user_name)
    logging.debug(cursor)

    # Execute SQL query to list all the Hive databases
    sql = "SHOW DATABASES"
    logging.info("Running the 'show dbs' query")
    cursor.execute(sql)
    results = cursor.fetchall()
    logging.debug(results)

    # For each database, record the storage path
    for tup in results:
        logging.info("Fetching details of database: " + tup[0])
        cursor.execute("DESCRIBE DATABASE EXTENDED " + tup[0])
        db_details = cursor.fetchall()

        # Create a DB metadata object
        hive_db = HiveDBMetadata()
        db_detail[2]

    logging.info("fetch_hive_dbs() end")
