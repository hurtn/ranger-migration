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
    ranger_database NVARCHAR(2000),
    Name NVARCHAR(100),
    Resources  NVARCHAR(2000),
    Paths  NVARCHAR(2000),
    Groups  NVARCHAR(2000),
    Users NVARCHAR(2000),
    Accesses  NVARCHAR(2000),
    permMapList nvarchar(4000),
    [Service Type]  NVARCHAR(100),
    Status  NVARCHAR(100),
    checksum NVARCHAR(400),
    CONSTRAINT "PK_Policies" PRIMARY KEY CLUSTERED ("ID") );

-- Create policy staging table
-- this stores the latest copy of all policies from ranger prior to merging with the main table
-- reason for this table is so that we can compare via the checksum column (a hash of all columns) 
-- whether any value has changed since the last time the process ran
-- These changed rows will then be merged ie upsert against the target table

create table ranger_policies_staging (
    ID int,
    ranger_database NVARCHAR(2000),
    Name NVARCHAR(100),
    Resources  NVARCHAR(2000),
    Paths  NVARCHAR(2000),
    Groups  NVARCHAR(2000),
    Users NVARCHAR(2000),
    Accesses  NVARCHAR(2000),
    permMapList nvarchar(4000)
    [Service Type]  NVARCHAR(100),
    Status  NVARCHAR(100),
    checksum NVARCHAR(400),
    CONSTRAINT "PK_Policies_Staging" PRIMARY KEY CLUSTERED ("ID") )

-- Enable CDC at the Database level
EXEC sys.sp_cdc_enable_db ;

-- Enable CDC on the policies table
EXEC sys.sp_cdc_enable_table
 @source_schema = 'dbo',
 @source_name = 'ranger_policies',
 @role_name = 'null',
 @supports_net_changes = 1;