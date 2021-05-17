 table policy_user;
create table policy_user (
    policy_id int,
    policy_users NVARCHAR(50),
    CONSTRAINT "PK_Customers" PRIMARY KEY CLUSTERED ("policy_id") 
);

select * from policy_user;

EXEC sys.sp_cdc_enable_db ;

EXEC sys.sp_cdc_enable_table
 @source_schema = 'dbo',
 @source_name = 'ranger_policies',
 @role_name = 'null',
 @supports_net_changes = 1;


 EXEC sys.sp_cdc_disable_table
 @source_schema = 'dbo',
 @source_name = 'ranger_policies',
 @capture_instance = 'all';

DECLARE  @from_lsn binary(10), @to_lsn binary(10);  
SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');  
SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal',  GETDATE());
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

select sys.fn_cdc_get_max_lsn ()
drop table policy_ctl
create table policy_ctl (
    ID int  NOT NULL    IDENTITY    PRIMARY KEY,
    application NVARCHAR(30),
    start_run date,
    end_run date,
    start_lsn binary,
    end_lsn  binary,
    rows_changes int,
    ACLs_changed int);
use policystore;
select sys.fn_cdc_get_min_lsn('dbo_ranger_policies') min_lsn, sys.fn_cdc_get_max_lsn() max_lsn ;

create table dummy (id int)
insert into dummy values (1)


select sys.fn_cdc_get_min_lsn('dbo_ranger_policies') min_lsn, sys.fn_cdc_get_max_lsn() max_lsn

drop table ranger_policies;
alter table ranger_policies alter column checksum nvarchar(1000)
create table ranger_policies (
    ID int,
    Name NVARCHAR(100),
    Resources  NVARCHAR(2000),
    Groups  NVARCHAR(2000),
    Users NVARCHAR(2000),
    Accesses  NVARCHAR(2000),
    [Service Type]  NVARCHAR(100),
    Status  NVARCHAR(100),
    checksum NVARCHAR(400),
    CONSTRAINT "PK_Policies" PRIMARY KEY CLUSTERED ("ID") )

,


select *  from ranger_policies;

update pi set checksum =  HASHBYTES('SHA1',  (select pi.id,pi.Name,pi.Resources,pi.Groups,pi.Users,pi.Accesses,pi.[Service Type],pi.Status for xml raw)) 
FROM
  dbo.ranger_policies pi

  update  dbo.ranger_policies set checksum =  HASHBYTES('SHA1',  (select id,Name,Resources,Groups,Users,Accesses,[Service Type],Status for xml raw)) 

 


update dbo.ranger_policies set checksum = (select HASHBYTES('SHA1', concat(NAME,resources) for xml raw))
SELECT HASHBYTES('SHA1', (SELECT * FROM dbo.ranger_policies pi  where pi.policy_id = po.policy_id FOR XML RAW));


MERGE policystore.dbo.ranger_policies_staging AS Target
            USING (select id,Name,Resources,Groups,Users,Accesses,[Service Type],Status, checksum from  policystore.dbo.ranger_policies
            ) AS Source
            ON (Target.[id] = Source.[id])
            WHEN MATCHED and Target.[checksum] <> source.[checksum] THEN
                UPDATE SET Target.[resources] = Source.[resources]
                        , Target.[Groups] = Source.[Groups]
                        , Target.[Users] = Source.[Users]
                        , Target.[Accesses] = Source.[Accesses]
                        , Target.[Status] = Source.[Status]
                        , Target.[Checksum] = Source.[Checksum]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([id],[Name], [Resources], [Groups],[Users],[Accesses],[Service Type],[Status])
                VALUES (
                 Source.[ID]
                , Source.[Name]
                , Source.[Resources]
                , Source.[Groups]
                , Source.[Users]
                , Source.[Accesses]
                , Source.[Service Type]
                , Source.[Status]
                );

TRUNCATE TABLE DBO.RANGER_POLICIES
DECLARE  @from_lsn binary(10), @to_lsn binary(10);  SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies')
                           SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [id],[Name],[Resources],[Groups],[Users],[Accesses] from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

            DECLARE  @from_lsn binary(10), @to_lsn binary(10);  
SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');  
SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal',  GETDATE());
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

            select [id],[Name],[Resources],[Groups],[Users],[Accesses] from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all');

            SELECT name, is_cdc_enabled FROM sys.databases;

            select sys.fn_cdc_get_max_lsn() from dummy

select sys.fn_cdc_increment_lsn(end_lsn) min_lsn,sys.fn_cdc_get_max_lsn() max_lsn from policystore.dbo.policy_ctl where id= (select max(id) from policystore.dbo.policy_ctl);