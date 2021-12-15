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
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
  
    initialise()

def initialise():
    logging.info('Initialise script starting...')
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    #logging.info("Connection string: " + connxstr)
    cnxn = pyodbc.connect(connxstr)

    stagingtablenm = "ranger_policies_staging"
    targettablenm = "ranger_policies"
    batchsize = 200
    params = urllib.parse.quote_plus(connxstr)
    collist = ['ID','Name','RepositoryName','Resources','Service Type','Status','permMapList']
    #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

    
    cursor = cnxn.cursor()
    try:
        initsql = """
        create table policy_ctl (
            ID int  NOT NULL    IDENTITY    PRIMARY KEY,
            application NVARCHAR(30),
            start_run datetime,
            end_run datetime,
            lsn_checkpoint datetime,
            rows_changed int,
            ACLs_changed int);

        """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
            #cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.warning('Warning occured while creating batch control table. Error message: '.join(sqlstate))

    try:
        # Create policy table
        # this stores the latest copy of all policies from ranger
        initsql = """
        create table ranger_policies (
            ID int,
            Name NVARCHAR(100),
            RepositoryName NVARCHAR(2000),   
            Resources  NVARCHAR(max),
            paths  NVARCHAR(max),
            permMapList nvarchar(max),
            Databases nvarchar(max),
            DB_Names nvarchar(max),
            isRecursive nvarchar(200),
            [Service Type]  NVARCHAR(100),
            Status  NVARCHAR(100),
            checksum NVARCHAR(400),
            tables NVARCHAR(max),
            table_type NVARCHAR(100),
            table_names  NVARCHAR(max),
            CONSTRAINT "PK_Policies" PRIMARY KEY CLUSTERED ("ID","RepositoryName") );
        """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
            #cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.warning('Warning occured while creating policies table. Error message: '.join(sqlstate))

    #try:
    #    # Create policy table
    #    # this stores the latest copy of all policies from ranger
    #    initsql = """
    #    create table ranger_policy_db_tables (
    #        Policy_ID int,
    #        RepositoryName NVARCHAR(2000),               
    #        Table_Name NVARCHAR(100),
    #        Path  NVARCHAR(max),
    #        CONSTRAINT "PK_Policy_db_tables" PRIMARY KEY CLUSTERED ("Policy_ID","RepositoryName","Table_Name") );
    #    """
    #    cursor.execute(initsql)
    #    cnxn.commit()

    #except pyodbc.DatabaseError as err:
    #        #cnxn.commit()
    #        sqlstate = err.args[1]
    #        sqlstate = sqlstate.split(".")
    #        logging.warning('Warning occured while creating policy database tables table. Error message: '.join(sqlstate))



    try:
        # Create policy transactions table
        # This table stores the permissions to be set on ADLS
        # The status represents whether the transaction can be ignored (for example where no valid principals were found), or whether it is pending, in progress or complete.
        initsql = """
        create table policy_transactions (
            ID int  NOT NULL    IDENTITY    PRIMARY KEY,
            policy_id int not null,
            storage_url nvarchar(max),
            adl_path  NVARCHAR(max),
            trans_type int,
            trans_action NVARCHAR(200),
            trans_mode NVARCHAR(200),
            acentry NVARCHAR(max),
            date_entered datetime,
            trans_status nvarchar(20),
            trans_reason nvarchar(max),
            continuation_token nvarchar(max),
            last_updated datetime,
            all_principals_excluded nvarchar(1),
            principals_excluded nvarchar(max),
            exclusion_list nvarchar(max),
            principals_included nvarchar(max),
            acl_count int,
            adl_permission_str nvarchar(3),
            permission_json nvarchar(max),
            depends_on int
        )
        """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating transactions table. Error message: '.join(sqlstate))

    try:
        # Create policy snapshot table
        # This table stores the current snapshot of paths and their associated principals and permissions 
        # To be used when performing business rule validation (e.g rule of maximum) and optimisation
        initsql = """
            create table policy_snapshot_by_path (
                ID int NOT NULL,    
                RepositoryName NVARCHAR(200),   
                adl_path  NVARCHAR(max),
                permMapList nvarchar(max),
                principal  nvarchar(max),
                permission nvarchar(max),
                audit_status  NVARCHAR(100),
                audit_date datetime)
            """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating policy snapshot table. Error message: '.join(sqlstate))

    try:
        # Create audit report table
        # This table stores the inconsistencies found during the audit/recon process. 
        # An entry in this table reflects an inconsistency by principal by permission by path
        initsql = """
                CREATE TABLE [dbo].[ranger_endpoints](
                    ID int  NOT NULL    IDENTITY    PRIMARY KEY,
                    endpoint  [nvarchar](2000) NOT NULL,
                    [last_polled] [datetime],
                    [last_poll_status] [nvarchar](50),
                    [last_status_change_date] datetime,
                    [max_retries] [int],
                    [retries] [int],
                    [failure_logic] [nvarchar](20),
                    [status] [nvarchar](20) NOT NULL,
                    date_entered datetime,
                    [added_by] [nvarchar](50)
                ) 
            """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating recon report table. Error message: '.join(sqlstate))


    try:
        # Create audit report table
        # This table stores the inconsistencies found during the audit/recon process. 
        # An entry in this table reflects an inconsistency by principal by permission by path
        initsql = """
                CREATE TABLE [dbo].[recon_report](
                    [audit_timestamp] [datetime] NOT NULL,
                    [adl_path] [nvarchar](max) NULL,
                    [principal] [nvarchar](2000) NULL,
                    [aad_oid] [nvarchar](200) NULL,
                    [adls_perm] [nvarchar](1) NULL,
                    [fullpath] [nvarchar](max) NULL,
                    [Inconsistency] [nvarchar](40) NOT NULL,
                    [resolved] [nvarchar](1),
                    [resolved_by] [nvarchar](50)
                ) 
            """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating recon report table. Error message: '.join(sqlstate))

    try:
        # Create policy staging table
        # this stores the latest copy of all policies from ranger prior to merging with the main table
        # reason for this table is so that we can compare via the checksum column (a hash of all columns) 
        # whether any value has changed since the last time the process ran
        # These changed rows will then be merged ie upsert against the target table

        initsql = """
        create table ranger_policies_staging (
            ID int,
            Name NVARCHAR(100),
            RepositoryName NVARCHAR(400),   
            Resources  NVARCHAR(max),
            Paths  NVARCHAR(max),
            Databases nvarchar(max),
            DB_Names nvarchar(max),
            isRecursive nvarchar(200),
            permMapList nvarchar(max),
            [Service Type]  NVARCHAR(100),
            Status  NVARCHAR(100),
            checksum NVARCHAR(400),
            tables NVARCHAR(max),
            table_type NVARCHAR(100),
            table_names  NVARCHAR(max),
            CONSTRAINT "PK_Policies_Staging" PRIMARY KEY CLUSTERED ("ID","RepositoryName")  )
        """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating staging table. Error message: '.join(sqlstate))
    
    # Enable CDC on the policies table. If running as managed identity, you need to impersonate the MI before running this statement. This requires giving the MI db_owner role. eg.ADD 
    # EXEC sp_addrolemember N'db_owner', N'policysyncdemoapp'
    # Then run the following with a priviledged user substituting the username below accordingly
    try:
        initsql = """
        EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = 'ranger_policies',
        @role_name = 'null',
        @supports_net_changes = 1;
        """
        cursor.execute(initsql)
        cnxn.commit()

    except pyodbc.DatabaseError as err:
        #cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while enabling CDC on policies table. Error message: '.join(sqlstate))
   
    # Create exclusions table
    # This table will store the principals to be excluded when ACLs are applied. Principal types include (U)sers and (G)roups
    try:
        initsql = """
        create table exclusions (
            ID int  NOT NULL    IDENTITY    PRIMARY KEY,
            type NVARCHAR(1),
            identifier NVARCHAR(100),
            date_entered datetime,
            entered_by NVARCHAR(100));

        """
        #logging.info("Truncating staging table: "+(truncsql))
        cursor.execute(initsql)
        cnxn.commit()
    except pyodbc.DatabaseError as err:
        cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating exclusions table. Error message: '.join(sqlstate))

    # Create permission mapping table
    # This table will store the ranger permissions to be converted to ADLS permissions
    try:
        initsql = """
          create table perm_mapping (
            ID int  NOT NULL    IDENTITY    PRIMARY KEY,
            ranger_perm NVARCHAR(100),
            adls_perm   NVARCHAR(100),
            date_entered datetime,
            entered_by NVARCHAR(100));
        """
        #logging.info("Truncating staging table: "+(truncsql))
        cursor.execute(initsql)
        # insert basic mappings, please customise as necessary
        insertsql = """insert into perm_mapping (ranger_perm, adls_perm) values ('select','r');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('read','r');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('update','w');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('write','w');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('execute','x');"""
        cursor.execute(insertsql)
        cnxn.commit()
    except pyodbc.DatabaseError as err:
        cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating permission mapping table. Error message: '.join(sqlstate))

    # Create AAD principal display name to OID cache table
    # This table is a performance optimisation to prevent having to make an AAD lookup for every principal
    try:
        initsql = """
          create table aad_cache (
            ID int  NOT NULL    IDENTITY    PRIMARY KEY,
            AAD_display_name NVARCHAR(256),
            AAD_OID   NVARCHAR(100),
            date_entered datetime,
            entered_by NVARCHAR(100));
        """
        #logging.info("Truncating staging table: "+(truncsql))
        cursor.execute(initsql)
        # insert basic mappings, please customise as necessary
        insertsql = """insert into perm_mapping (ranger_perm, adls_perm) values ('select','r');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('read','r');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('update','w');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('write','w');           
                        insert into perm_mapping (ranger_perm, adls_perm) values ('execute','x');"""
        cursor.execute(insertsql)
        cnxn.commit()
    except pyodbc.DatabaseError as err:
        cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.warning('Error occured while creating permission mapping table. Error message: '.join(sqlstate))


    logging.info('Initialise script complete')

initialise()



