import json
import logging
import pathlib

import jaydebeapi
# from security.keyvault import get_ms_credentials


# Class for storing the information about the Hive DB storage accounts
class HiveDBMetadata:

    def __init__(self, name, desc, path, owner_name, owner_type, parameters, ranger_policy_id, ranger_policy_name):
        self.name = name
        self.desc = desc
        self.path = path
        self.owner_name = owner_name
        self.owner_type = owner_type
        self.parameters = parameters
        self.ranger_policy_id = ranger_policy_id
        self.ranger_policy_name = ranger_policy_name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        self._desc = value

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value

    @property
    def owner_name(self):
        return self._owner_name

    @owner_name.setter
    def owner_name(self, value):
        self._owner_name = value

    @property
    def owner_type(self):
        return self._owner_type

    @owner_type.setter
    def owner_type(self, value):
        self._owner_type = value

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = value

    @property
    def ranger_policy_id(self):
        return self._ranger_policy_id

    @ranger_policy_id.setter
    def ranger_policy_id(self, value):
        self._ranger_policy_id = value

    @property
    def ranger_policy_name(self):
        return self._ranger_policy_name

    @ranger_policy_name.setter
    def ranger_policy_name(self, value):
        self._ranger_policy_name = value


# Gets the Hive metastore configurations set in the conf/metastore_conf.json file.
def get_ms_conf():
    data = None
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = str(proj_home_abs_path) + "/conf/metastore_conf.json"
    with open(conf_file_path) as json_file:
        data = json.load(json_file)
    logging.info("Read the Hive MS config")

    return data


# Connect to the Hive instance
def connect_hive(server, port, database, user_name, password):
    # Construct the JDBC connection string
    url = ("jdbc:hive2://" + server + ":" + str(port) + "/" + database +
           ";transportMode=http;ssl=true;httpPath=/hive2")

    # Connect to Hive
    conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, {'user': user_name, 'password': password})
    return conn.cursor()


# Fetches the Hive DB metadata from Hive MS for the databases in the Ranger policies & puts the same into the
# HiveDBMetadata object
def fetch_hive_dbs(ranger_hive_policies):
    logging.debug("fetch_hive_dbs() start")

    # Read in the configurations for metastore
    logging.info("Reading the hive ms config")
    ms_conf_dict = get_ms_conf()
    logging.debug(ms_conf_dict)

    # Use the metastore configs to connect to HDInsight Hive & get the cursor
    logging.info("Connecting to hive ms")
    cursor = connect_hive(ms_conf_dict["server"], ms_conf_dict["port"], ms_conf_dict["database"],
                          ms_conf_dict["user_name"], ms_conf_dict["password"])
    logging.debug(cursor)

    # Go through the loop of Ranger Hive policies
    ranger_hive_dbs = []
    for policy in ranger_hive_policies:
        db_name = policy.resource_name
        logging.info("Fetching details of database: " + db_name)
        cursor.execute("DESCRIBE DATABASE EXTENDED " + db_name)
        db_details = cursor.fetchall()

        # Create a DB metadata object
        hive_db = HiveDBMetadata(db_details[0], db_details[1], db_details[2], db_details[3], db_details[4],
                                 db_details[5], policy.policy_id, policy.policy_name)
        ranger_hive_dbs.append(hive_db)

    logging.debug("Ranger Hive DBs: " + ranger_hive_dbs)
    logging.debug("fetch_hive_dbs() end")
    return ranger_hive_dbs
