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
select sys.fn_cdc_get_min_lsn('dbo_ranger_policies');  
drop table test
create table test (cola nvarchar(42))
select UPPER(sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn('dbo_ranger_policies'))
select Convert(numeric(8,4),sys.fn_cdc_get_min_lsn('dbo_ranger_policies')) as [Varbinary to Numeric]
insert into test values (UPPER(sys.fn_varbintohexstr(sys.fn_cdc_get_min_lsn('dbo_ranger_policies'))))
select * from test
DECLARE  @from_lsn binary(10), @to_lsn binary(10);  
SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');  
SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal',  GETDATE());
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

--Convert from varbinary to numeric
Declare @varbinary2 as varbinary(max)
Set @varbinary2=0x08040001B471BC00
Select Convert(numeric(8,4),@varbinary2) as [Varbinary to Numeric]

select current_timestamp

select sys.fn_cdc_get_max_lsn ()
drop table policy_ctl
create table policy_ctl (
    ID int  NOT NULL    IDENTITY    PRIMARY KEY,
    application NVARCHAR(30),
    start_run datetime,
    end_run datetime,
    lsn_checkpoint datetime,
    end_lsn  nvarchar(42),
    rows_changed int,
    ACLs_changed int);
    select * from policy_ctl;
select sys.fn_cdc_map_time_to_lsn('smallest greater than', lsn_checkpoint) from policy_ctl;

    select sys.fn_cdc_increment_lsn(end_lsn) min_lsn,sys.fn_cdc_get_max_lsn() max_lsn from policystore.dbo.policy_ctl where id= (select max(id) from policystore.dbo.policy_ctl);
use policystore;
select sys.fn_cdc_get_min_lsn('dbo_ranger_policies') min_lsn, sys.fn_cdc_get_max_lsn() max_lsn ;
select * from policy_ctl
truncate table policy_ctl
create table dummy (id int)
insert into dummy values (1)


select sys.fn_cdc_get_min_lsn('dbo_ranger_policies') min_lsn, sys.fn_cdc_get_max_lsn() max_lsn

drop table ranger_policies;
alter table ranger_policies alter column checksum nvarchar(1000)
alter table ranger_policies add permMapList nvarchar(4000);
alter table ranger_policies_staging add permMapList nvarchar(4000);
update  ranger_policies set permMapList = '[{"userList":["nihurt_microsoft.com#EXT#_cooceltd.onmicrosoft.com#EXT#"],"groupList":["hr1","hr2"],"permList":["all"]},{"userList":[],"groupList":["hr3"],"permList":["select","read"]}]'
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


select *  from dbo.ranger_policies;
update  dbo.ranger_policies set groups =  'hr2,hr3' where groups = 'hr2.hr3'
drop table dbo.ranger_policies
select * from dbo.policy_ctl
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
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')  order by id,__$seqval,__$operation;

            select [id],[Name],[Resources],[Groups],[Users],[Accesses] from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all');

            SELECT name, is_cdc_enabled FROM sys.databases;

            select sys.fn_cdc_get_max_lsn() from dummy

select sys.fn_cdc_increment_lsn(end_lsn) min_lsn,sys.fn_cdc_get_max_lsn() max_lsn from policystore.dbo.policy_ctl where id= (select max(id) from policystore.dbo.policy_ctl);

insert into policystore.dbo.policy_ctl (application,start_run, end_run, start_lsn, end_lsn, rows_changed, acls_changed) values ('applyPolicies',getdate(),getdate(),'','',2,5985);
select * from policystore.dbo.policy_ctl;
select * FROM msdb.dbo.backupset;
exec sp_columns 'msdb.dbo.backupset';

DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-19 20:39:54')
SET @to_lsn = sys.fn_cdc_get_max_lsn()
select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_increment_lsn(sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-19 20:48:29'))
SET @to_lsn = sys.fn_cdc_get_max_lsn()
select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all');

select sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-19 21:48:29')
select current_timestamp;

update policy_ctl set lsn_checkpoint = '2021-05-19 20:48:29'

DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-19 20:59:01')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old');

            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-20 08:30:19')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select *
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old') order by __$seqval,id,__$operation;

            select sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-19 23:36:13')

            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-22 17:19:30')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;

            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');
                                               SET @to_lsn = sys.fn_cdc_get_max_lsn();
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;

            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');
                                               SET @to_lsn = sys.fn_cdc_get_max_lsn();
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;


            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-23 11:23:25')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;

            select * from dbo.policy_ctl

DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','2021-05-23 18:22:52')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;


            sys.fn_cdc_increment_lsn
            DECLARE  @from_lsn binary(10), @to_lsn binary(10); SET @from_lsn =sys.fn_cdc_increment_lsn(sys.fn_cdc_map_time_to_lsn('smallest less than equal','2021-05-24 13:20:40'))
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn()
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status]
            from cdc.fn_cdc_get_all_changes_dbo_ranger_policies(@from_lsn, @to_lsn, 'all update old')
            order by id,__$seqval,__$operation;

            select lsn_checkpoint from " + dbname + "." + dbschema + ".policy_ctl where id= (select max(id) from " + dbname + "." + dbschema + ".policy_ctl where application = '" + appname +"');

            select sys.fn_cdc_map_time_to_lsn('smallest greater than',lsn_checkpoint),sys.fn_cdc_get_max_lsn() from dbo.policy_ctl where id = (select max(id) from dbo.policy_ctl) where ;


            alter table 