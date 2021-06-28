import json
import logging
import pathlib

import jaydebeapi
# from security.keyvault import get_ms_credentials


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
    conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, driver_args={'user': user_name,
                                                                                   'password': password})
    return conn.cursor()


# Run some checks on the db_name parameter & returns a list of precise db names
def database_quality_check(db_name, cursor):
    all_dbs = []
    # db_name param could be a csv string. So, go around the loop to account for this
    split_db_names = db_name.split(",")
    for db in split_db_names:
        # For each db, now account for presence of wild chars like '*'
        if db.endswith("*"):
            cursor.execute('SHOW DATABASES LIKE "' + db + '"')
            show_db = cursor.fetchall()
            if show_db:
                for d in show_db:
                    all_dbs.append(d[0])
        else:
            all_dbs.append(db)
    return all_dbs


# Fetches the Hive DB metadata from Hive MS for the databases in the Ranger policies & puts the same into the
# HiveDBMetadata object
def fetch_hive_dbs(ranger_hive_policies):
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
    for json_policy in ranger_hive_policies:
        db_names = json_policy.databases
        # Perform some quality checks on the resource
        new_db_names = database_quality_check(db_names, cursor)
        if not new_db_names:
            # We'll log this and move on
            logging.info("Database quality check failed. Proceeding to the next policy.")
            continue
        else:
            for new_db_name in new_db_names:
                try:
                    logging.info("Fetching details of database: " + new_db_name)
                    cursor.execute("DESCRIBE DATABASE EXTENDED " + new_db_name)
                    db_details = cursor.fetchall()[0]
                    if db_details:
                        json_policy.set_hive_db_paths(db_details[2])
                        json_policy.set_hive_db_names(db_details[0])
                except:
                    logging.info("Exception while handling policy: " + str(json_policy))
                    continue
