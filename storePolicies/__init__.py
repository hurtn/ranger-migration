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
import sys
from tabulate import tabulate
sys.path.append( 'C:\workspace\centrica' )
from storePolicies import metastore


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    #jdk.install('11', jre=True)

    #opts = hive.HiveHelperOptions(
    #host="ar47-spark-esp-int.azurehdinsight.net",
    #port='443',
    #user='john.doe@ajithramanathoutlook.onmicrosoft.com',
    #password='Plok09ij',
    #hive_jar='./storePolicies/hive-jdbc-3.1.0.3.1.4.65-3-standalone.jar',
    #schema='transportMode=http;ssl=true;httpPath=/hive2',
    #)

    #h = hive.HiveHelper(database='default', opts=opts)
    #logging.info('Attempting to connect to hive')
    #h.connect()

    #tables = h.get_tables()
    
    #for t in tables:
    #    logging.info(t)
  
    storePolicies()

def storePolicies():
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    #logging.info("Connection string: " + connxstr)
    #logging.info("Connection string: " + connxstr)
    cnxn = pyodbc.connect(connxstr)
    samplefile=os.environ["samplefile"]
    try:
            # configure database params


            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr)
            collist = ['ID','Name','RepositoryName','Resources','Service Type','Status','permMapList']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            
            cursor = cnxn.cursor()
            truncsql = "TRUNCATE table " + dbname + "." + dbschema + "." + stagingtablenm  
            logging.info("Truncating staging table: "+(truncsql))
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

            
            #for csvpolicies in pd.read_csv(samplefile, chunksize=batchsize,names=collist):
              #hdfspolicies = csvpolicies[(csvpolicies['Service Type']=='hdfs')]
              #logging.info(hdfspolicies.head())
              #hdfspolicies.to_sql(stagingtablenm,engine,index=False,if_exists="append")
            
            hivepolicies = getRangerPolicies()
            allpolicies = hivepolicies #.append(hdfspolicies)
            allpolicies = allpolicies.astype(str)
            allpolicies = allpolicies.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove any unwanted spaces 
            allpolicies = allpolicies.applymap(lambda x: None if x=='nan' else x) #convert any nan strings to null
            allpolicies.replace({'\'': '"'}, regex=True)
            #logging.info(allpolicies.head())
            #logging.info(allpolicies.info())
            logging.info(tabulate(allpolicies, headers='keys', tablefmt='presto'))
            allpolicies.to_sql(stagingtablenm,engine,index=False,if_exists="append")
            

            sqltext = """select count(*) from """ + dbname + "." + dbschema + "." + stagingtablenm
            cursor.execute(sqltext)
            rowcount = cursor.fetchone()[0]
            logging.info(str(rowcount) + " records inserted into staging table")

            ## set the checksum on each record so we can use this to determine whether the record changed
            cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            updatesql = "update  " + dbname + "." + dbschema + "." + stagingtablenm  + " set [checksum] =  HASHBYTES('SHA1',  (select id,Name,RepositoryName,Resources,permMapList,[Service Type],Status,[paths],databases,db_names for xml raw)) "
            logging.info("Updating checksum: "+ updatesql)
            cursor.execute(updatesql)
            cnxn.commit()

            rowcount = -1
            mergesql = """MERGE """ + dbname + """.""" + dbschema + """.""" + targettablenm  + """ AS Target
            USING (select id,Name,RepositoryName,Resources,[Service Type],Status,[checksum],permMapList,paths,databases,db_names from  """ + dbname + """.""" + dbschema + """.""" + stagingtablenm  + """
            ) AS Source
            ON (Target.[id] = Source.[id] and Target.[RepositoryName]=Source.[RepositoryName])
            WHEN MATCHED AND Target.[checksum] <> source.[checksum] THEN
                UPDATE SET Target.[resources] = Source.[resources]
                        , Target.[Status] = Source.[Status]
                        , Target.[checksum] = Source.[checksum]
                        , Target.[permMapList] = Source.[permMapList]
                        , Target.[paths] = Source.[paths]
                        , Target.[databases] = Source.[databases]
                        , Target.[db_names] = Source.[db_names]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([id],[Name], [RepositoryName], [Resources],[Service Type],[Status],[checksum],[permMapList],[paths],[databases],db_names)
                VALUES (
                Source.[ID]
                , Source.[Name]
                , Source.[RepositoryName]
                , Source.[Resources]
                , Source.[Service Type]
                , Source.[Status]
                , Source.[checksum]
                , Source.[permMapList]
                , Source.[paths]
                , Source.[databases]
                , Source.[db_names]
                )
            WHEN NOT MATCHED BY SOURCE
                THEN DELETE; """
            #logging.info(mergesql)
            rowcount = cursor.execute(mergesql).rowcount
            cnxn.commit()
            logging.info(str(rowcount) + " rows merged into target policy table")

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            logging.info('Successfully processed file!')
    finally:
            cnxn.autocommit = True

def getRangerPolicies():
    rangercollist = ['policy_id','policy_name','repository_name','repository_type','perm_map_list','databases','is_enabled','is_recursive','paths','db_names']
    tgtcollist = ['ID','Name','RepositoryName','Service Type','permMapList','Databases','Status','isRecursive','paths','DB_Names']
    #rangerpolicies = pd.read_csv(r"sampledataframe.csv",names=rangercollist)
    rangerpolicies = metastore.get_ranger_policies_hive_dbs()
    logging.info(rangerpolicies.to_string())
    #hivepolicies = getRangerPolicies()
    #logging.info(hivepolicies.to_string())
    rangerpolicies.columns = tgtcollist
    logging.info(rangerpolicies.head())
    return rangerpolicies
    #for rangerpolicies in pd.read_csv(r"sampledataframe.csv", chunksize=200,names=rangercollist,header=0):
      #logging.info(rangerpolicies.head())
      #rangerpolicies.columns = tgtcollist
      #logging.info(rangerpolicies.head())
      # assign a source name is necessary
      #newdf = rangerpolicies.assign(RepositoryName='ranger1') 
      #return rangerpolicies

#storePolicies()
#getRangerPolicies()




