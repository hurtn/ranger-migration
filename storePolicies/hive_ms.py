import json
import logging
import pathlib
import pyodbc
import os
#import jaydebeapi
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
#def connect_hive(server, port, database, user_name, password):
    # Construct the JDBC connection string
    #url = ("jdbc:hive2://" + server + ":" + str(port) + "/" + database +
           #";transportMode=http;ssl=true;httpPath=/hive2")
    # Connect to Hive
    #conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, [user_name, password],"./dependencies/hive-jdbc-3.1.0.3.1.4.65-3-standalone.jar")
    #return conn.cursor()


def database_quality_check(db_name, cursor):
    #hiveconnxstr = "Driver={ODBC Driver 13 for SQL Server};Server=tcp:hiveextmsranger.database.windows.net,3342;Database=hivems;Uid=ajithr;Pwd=Qwe12rty!!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
    #cnxn = pyodbc.connect(hiveconnxstr)
    #cursor = cnxn.cursor()
    all_dbs = []
    # db_name param could be a csv string. So, go around the loop to account for this
    split_db_names = db_name.split(",")
    for db in split_db_names:
        # For each db, now account for presence of wild chars like '*'
        if db.endswith("*"):
            #cursor.execute('SHOW DATABASES LIKE "' + db + '"')
            sqltext = "select name from dbo.dbs where name like '" + db.rstrip('*') + "%'"
            #print("***************sql text " + sqltext)
            cursor.execute(sqltext)
            show_db = cursor.fetchall()
            #print("*************** results from hive **** " + str(show_db))
            if show_db:
                for d in show_db:
                    logging.info("Database="+d[0])
                    all_dbs.append(d[0])
        else:
            all_dbs.append(db)
    return all_dbs


# Fetches the Hive DB metadata from Hive MS for the databases in the Ranger policies & puts the same into the
# HiveDBMetadata object
def fetch_hive_dbs(ranger_hive_policies, servername):
    # Read in the configurations for metastore
    logging.info("Reading the hive ms config")
    ms_conf_dict = get_ms_conf()
    logging.debug(ms_conf_dict)
    #if servername:
    #  pservername = servername
    #else:
    pservername = ms_conf_dict["server"]
    # Use the metastore configs to connect to HDInsight Hive & get the cursor
    logging.info("Connecting to hive ms")
    #cursor = connect_hive(pservername, ms_conf_dict["port"], ms_conf_dict["database"],
                          #ms_conf_dict["user_name"], ms_conf_dict["password"])
    #logging.debug(cursor)

    # Go through the loop of Ranger Hive policies
    #hiveconnxstr = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:hiveextmsranger.database.windows.net,3342;Database=hivems;Uid=ajithr;Pwd=Qwe12rty!!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
    hiveconnxstr = os.environ["HiveDatabaseConnxStr"]
    cnxn = pyodbc.connect(hiveconnxstr)
    print("Connecting to hive " + hiveconnxstr)
    cursor = cnxn.cursor()
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
                    logging.info("Fetching details of hive database: " + new_db_name)
                    sqltext = "select db_location_uri from dbo.dbs where name ='" + new_db_name + "'"
                    cursor.execute(sqltext)
 
                    #cursor.execute("DESCRIBE DATABASE EXTENDED " + new_db_name)
                    db_details = cursor.fetchone()
 
                    if db_details:
                       logging.info('Found Hive database '+new_db_name+' is located at ' +str(db_details[0]) ) 
                       json_policy.set_hive_db_paths(str(db_details[0]))
                       json_policy.set_hive_db_names(new_db_name)
                    else:
                       logging.warn("Hive database "+new_db_name+" does not exist however is referenced in ranger under policy "+json_policy.policy_name)
                except:
                    logging.info("Exception while handling policy: " + str(json_policy.policy_name))
                    continue