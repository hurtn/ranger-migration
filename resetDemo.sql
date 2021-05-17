 EXEC sys.sp_cdc_disable_table
 @source_schema = 'dbo',
 @source_name = 'ranger_policies',
 @capture_instance = 'all';

 TRUNCATE TABLE DBO.RANGER_POLICIES;

 EXEC sys.sp_cdc_enable_table
 @source_schema = 'dbo',
 @source_name = 'ranger_policies',
 @role_name = 'null',
 @supports_net_changes = 1;