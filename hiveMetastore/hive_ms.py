import logging

import jaydebeapi

# Class for storing the information about the Hive DB storage accounts
from security.keyvault import get_ms_credentials


class HiveDBMetadata:
    name = "<<<__DEFAULT_NAME_PLACEHOLDER__>>>",
    desc = "<<<__DEFAULT_DESC_PLACEHOLDER__>>>",
    path = "<<<__DEFAULT_PATH_PLACEHOLDER__>>>",
    owner_name = "<<<__DEFAULT_OWNER_NAME_PLACEHOLDER__>>>",
    owner_type = "<<<__DEFAULT_OWNER_TYPE_PLACEHOLDER__>>>",
    parameters = "<<<__DEFAULT_PARAMETERS_PLACEHOLDER__>>>",
    ranger_policy_id = -12345

    def __init__(self, name, desc, path, owner_name, owner_type, parameters):
        self.name = name
        self.desc = desc
        self.path = path
        self.owner_name = owner_name
        self.owner_type = owner_type
        self.parameters = parameters

    def get_name(self):
        return self.name

    def get_desc(self):
        return self.desc

    def get_path(self):
        return self.path

    def get_owner_name(self):
        return self.owner_name

    def get_owner_type(self):
        return self.owner_type

    def get_parameters(self):
        return self.parameters

    def set_ranger_policy_id(self, policy_id):
        self.ranger_policy_id = policy_id


def read_ms_conf():
    ms_conf = []
    return ms_conf


# Connect to the Hive instance
def connect_hive(server, port, database, ms_key, user_name):
    # Construct the JDBC connection string
    url = ("jdbc:hive2://" + server + ":" + str(port) + "/" + database +
           ";transportMode=http;ssl=true;httpPath=/hive2")

    # Connect to HiveServer2
    # TESTING: conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, {'user': "admin",
    # 'password': "Qwe12rty!!"})
    passwd = get_ms_credentials(ms_key)
    conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, {'user': user_name,
                                                                       'password': passwd})
    return conn.cursor()


# Fetches all the Hive DBs from the metastore, puts the same into the HiveDBMetadata object
def fetch_hive_dbs(ms_conf_dict):
    cursor = connect_hive(ms_conf_dict.server, ms_conf_dict.port, ms_conf_dict.database, ms_conf_dict.user_name)

    # Execute SQL query to list all the Hive databases
    sql = "SHOW DATABASES"
    cursor.execute(sql)
    results = cursor.fetchall()

    # For each database, record the storage path
    for tup in results:
        logging.info("Fetching details of database: " + tup[0])
        cursor.execute("DESCRIBE DATABASE EXTENDED " + tup[0])
        db_details = cursor.fetchall()

        # Create a DB metadata object


        for db_detail in db_details:
            logging.info("Storage location of database " + tup[0] + " is " + db_detail[2])
