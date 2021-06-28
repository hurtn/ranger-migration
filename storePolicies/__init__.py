import os
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
from hive import HiveHelperOptions, HiveHelper


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    opts = HiveHelperOptions(
    host="ar78-spark-ex-hivems-ranger.azurehdinsight.net",
    port='443',
    user='admin',
    password='Qwe12rty!!',
    hive_jar='./storePolicies/hive-jdbc-3.1.0.3.1.4.65-3-standalone.jar',
    schema='transportMode=http;ssl=true;httpPath=/hive2',
    )

    h = HiveHelper(database='default', opts=opts)
    logging.info('Attempting to connect to hive')
    h.connect()

    tables = h.get_tables()
    
    for t in tables:
        print(t)
  
    storePolicies()

def storePolicies():
    connxstr=os.environ["DatabaseConnxStr"]
    #print("Connection string: " + connxstr)
    logging.debug("Connection string: " + connxstr)
    cnxn = pyodbc.connect(connxstr)
    dbname = 'policystore'

    try:
            # configure database params
            dbschema = "dbo"

            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr)
            collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status','permMapList']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            
            cursor = cnxn.cursor()
            truncsql = "TRUNCATE table " + dbname + "." + dbschema + "." + stagingtablenm  
            cursor.execute(truncsql)
            cnxn.commit()

            conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
            engine = create_engine(conn_str,echo=False)

            # sql alchemy listener
            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(
            conn, cursor, statement, params, context, executemany
                ):
                    if executemany:
                        cursor.fast_executemany = True

            
            for allpolicies in pd.read_csv(r"NHPolicySample.csv", chunksize=batchsize,names=collist):
              hdfspolicies = allpolicies[(allpolicies['Service Type']=='hdfs')]
              #print(hdfspolicies.head())
              hdfspolicies.to_sql(stagingtablenm,engine,index=False,if_exists="append")
            
          
            sqltext = """select count(*) from """ + dbname + "." + dbschema + "." + stagingtablenm
            cursor.execute(sqltext)
            rowcount = cursor.fetchone()[0]
            print(str(rowcount) + " records inserted")

            ## set the checksum on each record so we can use this to determine whether the record changed
            cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            updatesql = "update  " + dbname + "." + dbschema + "." + stagingtablenm  + " set checksum =  HASHBYTES('SHA1',  (select id,Name,Resources,permMapList,[Service Type],Status,[paths] for xml raw)) "
            cursor.execute(updatesql)
            cnxn.commit()

            rowcount = -1
            mergesql = """MERGE """ + dbname + """.""" + dbschema + """.""" + targettablenm  + """ AS Target
            USING (select id,Name,Resources,Groups,Users,Accesses,[Service Type],Status,Checksum,permMapList,paths from  """ + dbname + """.""" + dbschema + """.""" + stagingtablenm  + """
            ) AS Source
            ON (Target.[id] = Source.[id])
            WHEN MATCHED AND Target.checksum <> source.checksum THEN
                UPDATE SET Target.[resources] = Source.[resources]
                        , Target.[Groups] = Source.[Groups]
                        , Target.[Users] = Source.[Users]
                        , Target.[Accesses] = Source.[Accesses]
                        , Target.[Status] = Source.[Status]
                        , Target.[checksum] = Source.[checksum]
                        , Target.[permMapList] = Source.[permMapList]
                        , Target.[paths] = Source.[paths]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([id],[Name], [Resources], [Groups],[Users],[Accesses],[Service Type],[Status],[Checksum],[permMapList],[paths])
                VALUES (
                Source.[ID]
                , Source.[Name]
                , Source.[Resources]
                , Source.[Groups]
                , Source.[Users]
                , Source.[Accesses]
                , Source.[Service Type]
                , Source.[Status]
                , Source.[Checksum]
                , Source.[permMapList]
                , Source.[paths]
                )
            WHEN NOT MATCHED BY SOURCE
                THEN DELETE; """
            #print(mergesql)
            rowcount = cursor.execute(mergesql).rowcount
            cnxn.commit()
            print("rows merged "+str(rowcount))

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            print('Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            print('Successfully processed file!')
    finally:
            cnxn.autocommit = True

def fetchRangerPolicyByID(policyId):
  #usrPass = "admin:Qwe12rty!!"
  #b64Val = base64.b64encode(usrPass)
  endpoint = 'https://[clustername].azurehdinsight.net/ranger/service/public/api/policy/'+str(policyId)
  #headers={"Authorization": "Basic %s" % b64Val}
  r = requests.get(endpoint, auth=HTTPBasicAuth('admin', 'Qwe12rty!!'))
  response = r.json()
  json_formatted_str = json.dumps(response, indent=4)
  print(json_formatted_str)

storePolicies()



