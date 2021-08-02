-- Notes:
-- If using managed instance, ensure you have created a new database and have logged into that database using an AAD admin (set in the portal)
-- In order to use managed service identity authentication from your function app you will need to run the following script as that user (same name as the functino app) therefore
-- 1. ensure you have created a contained database user for the managed identity (same name as the function app) and given necessary privileges for example:
-- CREATE USER [name-of-your-function-app] FROM EXTERNAL PROVIDER;
-- GRANT CONTROL ON DATABASE::[database-name] TO [name-of-your-function-app];
-- EXEC sp_addrolemember N'db_owner', N'name-of-your-function-app'
-- 2. uncomment the line below and the last line in this script to run it as the managed identity
--execute as user = '[name-of-your-function-app]'

-- Table DDL required for Ranger Migration / Synchronisation App
-- See the github repo for more details: https://github.com/hurtn/ranger-migration

-- Create control table
-- This table will store various control information but importantly the LSN datatime checkpoint of the last run
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

--revert
