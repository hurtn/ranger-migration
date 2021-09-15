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
import sys


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
  
    initialise()

def initialise():
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    #logging.info("Connection string: " + connxstr)
    #logging.info("Connection string: " + connxstr)
    cnxn = pyodbc.connect(connxstr)
    try:
            # configure database params


            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr)
            collist = ['ID','Name','RepositoryName','Resources','Service Type','Status','permMapList']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            
            cursor = cnxn.cursor()
            truncsql = """
            create table policy_ctl (
                ID int  NOT NULL    IDENTITY    PRIMARY KEY,
                application NVARCHAR(30),
                start_run datetime,
                end_run datetime,
                lsn_checkpoint datetime,
                rows_changed int,
                ACLs_changed int);

            -- Create policy table
            -- this stores the latest copy of all policies from ranger
            create table ranger_policies (
                ID int,
                Name NVARCHAR(100),
                RepositoryName NVARCHAR(2000),   
                Resources  NVARCHAR(2000),
                paths  NVARCHAR(4000),
                permMapList nvarchar(4000),
                Databases nvarchar(4000),
                DB_Names nvarchar(4000),
                isRecursive nvarchar(200),
                [Service Type]  NVARCHAR(100),
                Status  NVARCHAR(100),
                checksum NVARCHAR(400),
                CONSTRAINT "PK_Policies" PRIMARY KEY CLUSTERED ("ID","RepositoryName") );

            -- Create policy staging table
            -- this stores the latest copy of all policies from ranger prior to merging with the main table
            -- reason for this table is so that we can compare via the checksum column (a hash of all columns) 
            -- whether any value has changed since the last time the process ran
            -- These changed rows will then be merged ie upsert against the target table

            create table ranger_policies_staging (
                ID int,
                Name NVARCHAR(100),
                RepositoryName NVARCHAR(2000),   
                Resources  NVARCHAR(2000),
                Paths  NVARCHAR(2000),
                Databases nvarchar(4000),
                DB_Names nvarchar(4000),
                isRecursive nvarchar(200),
                permMapList nvarchar(4000),
                [Service Type]  NVARCHAR(100),
                Status  NVARCHAR(100),
                checksum NVARCHAR(400),
                CONSTRAINT "PK_Policies_Staging" PRIMARY KEY CLUSTERED ("ID","RepositoryName")  )

            -- Enable CDC at the Database level
            EXEC sys.sp_cdc_enable_db ;

            -- Enable CDC on the policies table. If running as managed identity, you need to impersonate the MI before running this statement. This requires giving the MI db_owner role. eg.ADD 
            --EXEC sp_addrolemember N'db_owner', N'policysyncdemoapp'
            --Then run the following with a priviledged user substituting the username below accordingly

            EXEC sys.sp_cdc_enable_table
            @source_schema = 'dbo',
            @source_name = 'ranger_policies',
            @role_name = 'null',
            @supports_net_changes = 1;

            """
            #logging.info("Truncating staging table: "+(truncsql))
            cursor.execute(initsql)
            cnxn.commit()




