import datetime
import logging
import urllib
import pyodbc
import pandas as pd
import pandas.io.common
from sqlalchemy import create_engine
from sqlalchemy import event
import sqlalchemy
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    #storePolicy()
   

def applyPolicies():
    try:
                # configure database params
            dbname = "policystore"
            dbschema = "dbo"
            connxstr="Driver={ODBC Driver 13 for SQL Server};Server=tcp:cenpolicystor.public.ab33566069d1.database.windows.net,3342;Database="+dbname+";Uid=saadmin;Pwd=Obv10us123456789;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"

            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            get_ct_info = "select sys.fn_cdc_increment_lsn(end_lsn) min_lsn,sys.fn_cdc_get_max_lsn() max_lsn from " + dbname + "." + dbschema + ".policy_ctl where id= (select max(id) from " + dbname + "." + dbschema + ".policy_ctl);"
            print(get_ct_info)
            cursor.execute(get_ct_info)
            row = cursor.fetchone()
            start_lsn =None
            if row:
                print(row[1])
                start_lsn  = row[0]
                end_lsn =  row[1]
#            else:
 #               get_ct_info = "select sys.fn_cdc_get_min_lsn('dbo_ranger_policies') min_lsn, sys.fn_cdc_get_max_lsn() max_lsn from " + dbname + "." + dbschema + ".dummy"
 #               print(get_ct_info)
 #               print("1")
 #               cursor.execute(get_ct_info)
 #               print("2)")
 #               row = cursor.fetchone()
 #               if row:
 #                   print('no previous lsn ' +row[0])
 #                   start_lsn  = row[0]
 #                   end_lsn =  row[1]
 #               else:
 #                    raise Exception("Could not obtain LSN values. Please ensure CDC is enabled on the table")                                  
               
            changessql = "DECLARE  @from_lsn binary(10), @to_lsn binary(10); " 
            if start_lsn is not None:
              changessql = changessql + """SET @from_lsn =""" + start_lsn + """
                                        SET @to_lsn = """ +  end_lsn

            else: 
                   changessql = changessql + """SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');
                                               SET @to_lsn = sys.fn_cdc_get_max_lsn(); """
            changessql = changessql + """            
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status] 
            from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all');"""
            #changessql = "SELECT name, db_name() FROM sys.databases"
            print(changessql)
            #cursor.execute(changessql)
            #row = cursor.fetchone()
            #while row:
                #print(str(row[1]))
                #row = cursor.fetchone()

            changesdf= pandas.io.sql.read_sql(changessql, cnxn)
            #print(changesdf)
            insertdf = changesdf[(changesdf['__$operation']==2)]
            print(insertdf)
#    except pyodbc.DatabaseError as err:
 #           cnxn.commit()
  #          sqlstate = err.args[1]
   #         sqlstate = sqlstate.split(".")
    #        print('Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    #else:
     #       cnxn.commit()
      #      print('Successfully processed file!')
    finally:
            cnxn.autocommit = True

applyPolicies()

