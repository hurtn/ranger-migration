#MIT License
#
#Copyright (c) 2021 Nick Hurt
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import json
import logging
import pathlib
import pyodbc
import os


# Gets the Hive metastore configurations set in the conf/metastore_conf.json file.
def get_ms_conf():
    data = None
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = str(proj_home_abs_path) + "/conf/metastore_conf.json"
    with open(conf_file_path) as json_file:
        data = json.load(json_file)
    logging.info("Read the Hive MS config")

    return data

def database_quality_check(db_name, cursor):
    all_dbs = []
    # db_name param could be a csv string. So, go around the loop to account for this
    split_db_names = db_name.split(",")
    for db in split_db_names:
        # For each db, now account for presence of wild chars like '*'
        if db.endswith("*"):
            sqltext = "select name from dbo.dbs where name like '" + db.rstrip('*') + "%'"
            cursor.execute(sqltext)
            show_db = cursor.fetchall()
            if show_db:
                for d in show_db:
                    #logging.info("Database="+d[0])
                    all_dbs.append(d[0])
        else:
            all_dbs.append(db)
    return all_dbs


# Fetches the Hive DB metadata from Hive MS for the databases in the Ranger policies & puts the same into the

def fetch_hive_dbs(ranger_hive_policies, servername):
    # Go through the loop of Hive databases and tables from the "local" data copy
    hiveconnxstr= os.environ["DatabaseConnxStr"]

    cnxn = pyodbc.connect(hiveconnxstr)

    cursor = cnxn.cursor()
    for json_policy in ranger_hive_policies:
        logging.info("Lookup Hive metadata for policy ID "+ str(json_policy.policy_id))
        db_names = json_policy.databases
        # Perform some quality checks on the resource
        new_db_names = database_quality_check(db_names, cursor)
        if not new_db_names:
            # We'll log this and move on
            logging.warn("Database quality check failed. Proceeding to the next policy.")
            continue
        else:
            for new_db_name in new_db_names:
                logging.info("POlicy ID "+ str(json_policy.policy_id) + " for db " + new_db_name)
                try:
                    #logging.info("Fetching details of hive database: " + new_db_name)
                    sqltext = "select db_location_uri from dbo.dbs where name ='" + new_db_name + "'"
                    cursor.execute(sqltext)
                    db_details = cursor.fetchone()
                    if db_details:
                       logging.info('hive_ms.py.fetch_hive_dbs(): Found Hive database '+new_db_name+' is located at ' +str(db_details[0]) ) 
                       json_policy.set_hive_db_paths(str(db_details[0]))
                       json_policy.set_hive_db_names(new_db_name)
                    else:
                       logging.warn("hive_ms.py.fetch_hive_dbs(): Hive database "+new_db_name+" does not exist however is referenced in ranger under policy "+json_policy.policy_name)

                    # If table specific exclusions apply then fetch all tables and locations for the current database
                    if json_policy.table_type == "Exclusion" and json_policy.tables != "*": 
                        #obtain all tables+paths for the database so that we can exclude the tables+paths specified in the exclusion
                        sqltext = "select t.tbl_name, sds.location from DBS d inner join tbls t on d.DB_ID = t.DB_ID  inner join sds on sds.sd_id = t.sd_id where d.name ='" + new_db_name + "'"
                        cursor.execute(sqltext)
                        row = cursor.fetchone()
                        while row:
                            json_policy.set_hive_tbl_names(row[0],row[1])
                            row = cursor.fetchone()
                except pyodbc.DatabaseError as err:
                            cnxn.commit()
                            sqlstate = err.args[1]
                            sqlstate = sqlstate.split(".")
                            logging.error('hive_ms.py.fetch_hive_dbs(): Error occured while fetching policy details using sql '+sqltext+'. Rollback. Error message: '.join(sqlstate))
                except:
                    logging.info("hive_ms.py.fetch_hive_dbs(): Exception while handling policy: " + str(json_policy.policy_name))
                    continue
    