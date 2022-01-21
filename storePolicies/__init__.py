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
import pymysql
import pandas as pd
import pandas.io.common
from sqlalchemy import create_engine
from sqlalchemy import event
import sqlalchemy
import azure.functions as func
import sys
from tabulate import tabulate
sys.path.append( 'C:\workspace\centrica' )
#from storePolicies import metastore
from storePolicies.hive_ms import fetch_hive_dbs
from storePolicies.ranger import fetch_ranger_hive_dbs



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
  
    storePolicies()

def storePolicies():
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    allpolicies = pd.DataFrame()
    rangerpolicies = pd.DataFrame()
    tgtcollist = ['ID','Name','RepositoryName','Service Type','permMapList','Databases','Status','isRecursive','paths','DB_Names','tables','table_type','table_names']

    cnxn = pyodbc.connect(connxstr)
    rangerendpoints = []
    try:

        # fetch a local copy of the main hive tables
        hiveconnxstr = os.environ["HiveDatabaseConnxStr"]
        hiveparams = urllib.parse.quote_plus(hiveconnxstr)
        #hive_conn_str needs to be in SQLAlchemy format eg  username:password@FQDN_or_IP:port
        hive_conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(hiveparams)#'mysql+pymysql://' + hiveconnxstr + '/metastore?charset=utf8mb4' 
        cursor = cnxn.cursor()
        hive_engine = create_engine(hive_conn_str,echo=False).connect()
        # we keep a "local" copy of all hive tables and refresh them every time this app runs
        # read all required Hive tables into dataframe so  we can store in working database
        hivedbsdf = pd.read_sql_query('select * from dbs', hive_engine)
        hivetblsdf = pd.read_sql_query('select * from tbls', hive_engine)
        hivesdsdf = pd.read_sql_query('select * from sds', hive_engine)
        #logging.info("Output hive dbs table:")
        #logging.info(tabulate(hivedf, headers='keys', tablefmt='presto'))

        stagingtablenm = "ranger_policies_staging"
        targettablenm = "ranger_policies"
        batchsize = 200
        params = urllib.parse.quote_plus(connxstr)

        conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
        engine = create_engine(conn_str,echo=False)

        # sql alchemy listener
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True

        # loading hive tables
        logging.info("storePolicies: Loading a copy of Hive tables into working database")
        hivedbsdf.to_sql("dbs",engine,index=False,if_exists="replace")
        hivetblsdf.to_sql("tbls",engine,index=False,if_exists="replace")
        hivesdsdf.to_sql("sds",engine,index=False,if_exists="replace")


        # clear out the staging table, ready for loading...                    
        truncsql = "TRUNCATE table " + dbname + "." + dbschema + "." + stagingtablenm  
        #logging.info("Truncating staging table: "+(truncsql))
        cursor.execute(truncsql)
        cnxn.commit()

        # fetch all the ranger endpoints
        fetchendpoints = "select endpoint from  " + dbname + "." + dbschema + ".ranger_endpoints where status ='live'"
        #logging.info("Truncating staging table: "+(truncsql))
        cursor.execute(fetchendpoints)
        row = cursor.fetchone()
        while row:
            rangerendpoints.append(str(row[0]))

            endpoint = row[0]
            logging.info("Connecting to ranger store @ "+endpoint)  # remember to add -int.azurehdinsight.net to your server name if you wish to connect to the ranger store on HDI using the private address
            # Connect to Ranger and fetch the Hive policy details in a list
            ranger_hive_policies = fetch_ranger_hive_dbs(endpoint)
            #logging.debug(str(ranger_hive_policies)) 
        
            # Now connect to Hive and fetch the database metadata details in a list
            fetch_hive_dbs(ranger_hive_policies)

            pd.set_option("display.max_columns", None)
            rangerpolicies = pd.DataFrame([x.as_dict() for x in ranger_hive_policies])
            #rangerpolicies = rangerpolicies.append(ranger_pdf) 


            #logging.info(rangerpolicies.to_string())
            rangerpolicies.columns = tgtcollist
            #allpolicies.append(rangerpolicies)
        
            rangerpolicies = rangerpolicies.astype(str)
            rangerpolicies = rangerpolicies.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove any unwanted spaces 
            rangerpolicies = rangerpolicies.applymap(lambda x: None if x=='nan' else x) #convert any nan strings to null
            rangerpolicies.replace({'\'': '"'}, regex=True)
            # uncomment this next line if you want to see the output of the policies in formatted results
            #logging.info(tabulate(rangerpolicies, headers='keys', tablefmt='presto'))

            # This to_sql method is a SQL alchemy method to fast load bulk data from a pandas dataframe into the staging table. Column names need to match up for the correct mapping.
            logging.info("Loading staging table with data from "+ str(row[0]))
            rangerpolicies.to_sql(stagingtablenm,engine,index=False,if_exists="append")
            row = cursor.fetchone()
        
        sqltext = """select count(*) from """ + dbname + "." + dbschema + "." + stagingtablenm
        cursor.execute(sqltext)
        rowcount = cursor.fetchone()[0]
        logging.info(str(rowcount) + " records inserted into staging table")

        if rowcount>0:

            #comment these lines if you are not using local testing i.e. running in Azure Function app with the sample spreadsheet
            #samplefile = "NHPolicySample.csv"
            #pd.options.mode.chained_assignment = None  # default='warn'
            #collist = ['ID','Name','RepositoryName','Resources','Service Type','Status','permMapList']
            #for csvpolicies in pd.read_csv(samplefile, chunksize=batchsize,names=collist):
                #allpolicies = csvpolicies[(csvpolicies['Service Type']=='hive')]
                ##for value in allpolicies.Resources():
                #allpolicies['paths'] = allpolicies['Resources']
                ##print(allpolicies.head())
                ##hdfspolicies.to_sql(stagingtablenm,engine,index=False,if_exists="append")
            
            #comment these two lines if you are running locally with the spreadsheet input and no hive / ranger deployment to poll
            #hivepolicies = getRangerPolicies()

            ## set the checksum on each record so we can use this to determine whether the record changed
            updatesql = "update  " + dbname + "." + dbschema + "." + stagingtablenm  + " set [checksum] =  HASHBYTES('SHA1',  (select id,Name,RepositoryName,Resources,permMapList,[Service Type],Status,[paths],databases,db_names,tables,table_type,table_names for xml raw)) "
            #logging.info("Updating checksum: "+ updatesql)
            cursor.execute(updatesql)
            cnxn.commit()

            rowcount = -1
            mergesql = """MERGE """ + dbname + """.""" + dbschema + """.""" + targettablenm  + """ AS Target
            USING (select id,Name, RepositoryName,Resources,[Service Type],Status,[checksum],permMapList,paths,databases,db_names,tables,table_type,table_names from  """ + dbname + """.""" + dbschema + """.""" + stagingtablenm  + """
             where [databases]!='information_schema' and [paths] !='' and [Name] not in (select identifier from  """ + dbname + """.""" + dbschema + """.exclusions where type = 'P')) AS Source
            ON (Target.[id] = Source.[id] and Target.[RepositoryName]=Source.[RepositoryName])
            WHEN MATCHED AND Target.[checksum] <> source.[checksum] THEN
                UPDATE SET Target.[resources] = Source.[resources]
                        , Target.[Status] = Source.[Status]
                        , Target.[checksum] = Source.[checksum]
                        , Target.[permMapList] = Source.[permMapList]
                        , Target.[paths] = Source.[paths]
                        , Target.[databases] = Source.[databases]
                        , Target.[db_names] = Source.[db_names]
                        , Target.[tables] = Source.[tables]
                        , Target.[table_type] = Source.[table_type]
                        , Target.[table_names] = Source.[table_names]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([id],[Name], [RepositoryName], [Resources],[Service Type],[Status],[checksum],[permMapList],[paths],[databases],db_names,tables,table_type,table_names)
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
                , Source.[tables]
                , Source.[table_type]
                , Source.[table_names]
                )
            WHEN NOT MATCHED BY SOURCE
                THEN DELETE; """
            #logging.info(mergesql)
            rowcount = cursor.execute(mergesql).rowcount
            cnxn.commit()
            logging.info(str(rowcount) + " rows merged into target policy table")
            
            #if samplefile:
            #    localdevsql  = """update ranger_policies set paths = concat('abfs://ar12-spark-esp-2021-09-02t10-13-26-963z@hdiprimaryranger.dfs.core.windows.net/datalake/dimensions',replace(replace(resources,'path=[',''),']',''))"""
            #    rowcount = cursor.execute(localdevsql).rowcount
            #    cnxn.commit()
            #    logging.info(str(rowcount) + " paths updated in policies table")

            #select trim(value) from [dbo].[policy_snapshot_by_path] cross apply
            # STRING_SPLIT(replace(replace(replace(adl_path,'[',''),']',''),'''',''),',')

            clearsnapshot  = """truncate table policy_snapshot_by_path"""
            cursor.execute(clearsnapshot)
            cnxn.commit()
            snapshotsql = """insert into policy_snapshot_by_path (ID, RepositoryName,adl_path,permMapList,principal,permission)
                             select distinct id, repositoryname,  replace(trim(pathdata.value),'hdfs://namenode:9000/user/hive/warehouse/','https://rangersync.dfs.core.windows.net/datalake/')  adl_path,  permmaplist, userdata.value principal, permdata.value permission from ranger_policies as Tab
                             cross apply openjson (replace(Tab.permMapList,'''','"')) as jsondata 
                             cross apply openjson(jsondata.value, '$.userList') as userdata
                             cross apply openjson(jsondata.value,'$.permList') as permdata
                             cross apply STRING_SPLIT(replace(replace(replace(paths,'[',''),']',''),'''',''),',') as pathdata
                             where userdata.value is not null and table_type != 'Exclusion'
                             and status = 'True'
                             UNION
                             select distinct id, repositoryname,  replace(trim(pathdata.value),'hdfs://namenode:9000/user/hive/warehouse/','https://rangersync.dfs.core.windows.net/datalake/')  adl_path, permmaplist, groupdata.value principal, permdata.value permission  from ranger_policies as Tab
                             cross apply openjson (replace(Tab.permMapList,'''','"')) as jsondata 
                             cross apply openjson(jsondata.value, '$.groupList') as groupdata
                             cross apply openjson(jsondata.value,'$.permList') as permdata
                             cross apply STRING_SPLIT(replace(replace(replace(paths,'[',''),']',''),'''',''),',') as pathdata
                             where groupdata.value is not null and table_type != 'Exclusion'
                             and status = 'True'
                             UNION
                             select distinct id, repositoryname,  replace(trim(tbldata.value),'hdfs://namenode:9000/user/hive/warehouse/','https://rangersync.dfs.core.windows.net/datalake/') adl_path,  permmaplist, userdata.value principal, permdata.value permission from ranger_policies as Tab
                             cross apply openjson (replace(Tab.permMapList,'''','"')) as jsondata 
                             cross apply openjson(jsondata.value, '$.userList') as userdata
                             cross apply openjson(jsondata.value,'$.permList') as permdata
                             cross apply openjson (Tab.table_names) as tbldata 
                             where userdata.value is not null and table_type = 'Exclusion'
                             and status = 'True'
                             and tables COLLATE DATABASE_DEFAULT !=  tbldata.[key] COLLATE DATABASE_DEFAULT
                             UNION
                             select distinct id, repositoryname, replace(trim(tbldata.value),'hdfs://namenode:9000/user/hive/warehouse/','https://rangersync.dfs.core.windows.net/datalake/')  adl_path, permmaplist, groupdata.value principal, permdata.value permission  from ranger_policies as Tab
                             cross apply openjson (replace(Tab.permMapList,'''','"')) as jsondata 
                             cross apply openjson(jsondata.value, '$.groupList') as groupdata
                             cross apply openjson(jsondata.value,'$.permList') as permdata
                             cross apply openjson (Tab.table_names) as tbldata 
                             where groupdata.value is not null and table_type = 'Exclusion'
                             and status = 'True'
                             and tables COLLATE DATABASE_DEFAULT !=  tbldata.[key] COLLATE DATABASE_DEFAULT
                             """
                             # first insert where type is not exclusion and tables == *
                             #second insert where type is exclusion and tables != * joined to db_table lookup table excluded the ones in the exclusion list

            rowcount = cursor.execute(snapshotsql).rowcount
            cnxn.commit()
            print(str(rowcount) + " rows saved to snapshot table")



    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error occured while storing ranger policies. Rollback. Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            logging.info('Successfully stored ranger policies')
    finally:
            cnxn.autocommit = True


#storePolicies()
#getRangerPolicies()




