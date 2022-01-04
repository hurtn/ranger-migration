This section describes the various tables and columns, their purpose and possible values.

policy_ctl
----------
This table is auto-populated by the respective application and is primarily used to keep track of database LSN (logical sequence number) timestamp of each scheduled run. This is used to determine the set of changes since the last time the function app ran. Other uses include store metrics and information associated with each run.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| application | nvarchar(30) | Y | Name of the application e.g. storePlicies, applyPolicies |
| start_run | datetime | Y | Start timestamp of the run |
| end_run | datetime | Y | End timestamp of the run | 
| lsn_checkpoint | datetime | N | Timestamp of the last run |
| rows_changed | int | N | The number of rows merged in the policy table. Applicable only to the storePolicies application which loads the policies from Ranger and stores them in the policy table |
| ACLs_changed | int | N | The total number of ACLs changed during each run. Applicable only to the applyPolicies application which converts the policies into transactions, applies validation and business logic before sending transactions to the queue for processing |

exclusions
----------
This table should be manually populated and is used to store policy or principal exclusions, i.e. these are policies or principals (users and groups) which will be excluded/ignored. This may be useful for policies which apply to Hive users only and not to the underlying ADLS data or groups which are administrators of the Ranger service which shouldn't have access to the underlying data. 
For policies the exact policy name and matching case should by provided to exclude policies with a given a name. For users provided the user principal name, and for groups provide the security group name. For users and groups, case is ignored.   

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| type | nvarchar(1) | Y | The type of exclusion. Possible values are "P" for policy or "U" for user or "G" for group.
| identifier | nvarchar(100) | Y | The name of either the policy, user principal name, or group. |
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |

Example entries include:
insert into exclusions (type, identifier) values ('U','ADMIN')
insert into exclusions (type, identifier) values ('G','Ranger Admins')
insert into exclusions (type, identifier) values ('P','Information_schema database tables columns') 
insert into exclusions (type, identifier) values ('P','all - database') 



perm_mapping
------------

This table has defaults which should be reviewed and stores the mapping of Ranger permissions to ADLS equivalent permissions. Needs to be populated prior to running the application therefore defaults are provided, see below.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| ranger_perm | nvarchar(100) | Y | The ranger permission value e.g. select, read, update, write, execute |
| adls_perm | nvarchar(1) | Y | The ADLS equivalent permission. Possible values are "r", "w" or "x" |
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |

The following inserts are default and added in the first run initialise application, please customise as necessary:

insert into perm_mapping (ranger_perm, adls_perm) values ('select','r')           
insert into perm_mapping (ranger_perm, adls_perm) values ('read','r')           
insert into perm_mapping (ranger_perm, adls_perm) values ('update','w')           
insert into perm_mapping (ranger_perm, adls_perm) values ('write','w')           
insert into perm_mapping (ranger_perm, adls_perm) values ('execute','x') 

aad_cache
---------

This table is automatically populated and is a cache of all OIDs from the last apply policies run. It is used as an optimisation so as not to repeatedly query the AAD API for the same user in a given run and is also used in the recon report to translate OIDs (provided by the ADLS API) back to principal names which are compared with the principal names stored in ranger policies.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| AAD_principal_name | nvarchar(256) | Y | The user principal name |
| AAD_OID | nvarchar(100) | Y | The ADLS equivalent permission. Possible values are "r", "w" or "x" |
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |

ranger_endpoints
----------------

This table is manually populated and stores the ranger service details. After each endpoint is queried the result are persisted to the database.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| endpoint |nvarchar(2000) | Y | The URL of the ranger endpoint to query. |
| last_polled |datetime | N | The last date time the endpoint was queried. |
| last_poll_status | nvarchar(50) | N | The status of the last HTTP GET request from this endpoint. |
| last_status_change_date | datetime | N | The last time the HTTP GET status changed. |
| max_retries | int | N | When a non-200 response is obtained, this value is the maximum number of times the endpoint retried | 
| retries | int | N | The current number of retry attempts |
| failure_logic | nvarchar(20) | N | TBD: this value will store the logic associated with an endpoint which has exceed maximum retries i.e. delete all policies and remove permissions or leave in place.
| status | nvarchar(20) | Y | The status of the service endpoint. Only those endpoints with a status of "live" are queried.   
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |

policy_snapshot_by_path
-----------------------
This table is automatically populated and stores the current snapshot of policies by path, by permission, by principal. It is used when performing business rule validation (e.g rule of maximum) and reconcilliation between ADLS and ranger permissions.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| RepositoryName | NVARCHAR(200) | N | The ranger repository / service name |   
| adl_path | NVARCHAR(max) | N | The hive location where the underlying data is stored |
| permMapList | nvarchar(max) | N | The permaplist is the json array of permissions for a given resource as provided by ranger |  
| principal | nvarchar(max) | N | The principal from the permaplist for this record |
| permission | nvarchar(max) | N | The ranger permission granted to this principal | 
| audit_status | nvarchar(100) | N | TBD The recon status after this permission entry has been compared with ADLS | 
| audit_date | datetime | N | TBD The date this entry was audtied |


ranger_policies
---------------

This table is automatically populated with the policies found when polling the ranger endpoints.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| Name | nvarchar(100) | N | Policy name as obtained from Ranger |
| RepositoryName | nvarchar(2000) | Y | Primary key. Name of the service therefore it is recommended that each service name is unique across all the provided endpoints |   
| Resources | nvarchar(max) | N | The path which the permission has been granted on. Only applicable to HDFS policies.
| paths | nvarchar(max) | N | The path which the permission has been granted on. Ojnly applicable to Hive policies.
| permMapList | nvarchar(max) | N | The permaplist is the json array of permissions for a given resource as provided by ranger |
| Databases | nvarchar(max) | N | The list of databases this policy applies to as obtained from Ranger i.e. can include a wildcard |
| DB_Names | nvarchar(max) | N | The derived value of comma seperated database name. Where a wildcard was used this will contain the full database names extracte |
| isRecursive | nvarchar(200) | N | Whether the policy is recursive as obtained from Ranger |
| Service Type | nvarchar(100) | N | Hive or HDFS |
| Status | nvarchar(100) | N | Whether the policy is active. Values are either True or False |
| checksum | nvarchar(400) | N | The checksum is a derived field calculated by running Ranger supplied values through a SHA1 algo to obtain a hash. This has compared to determine whether the record has changed |
| tables | nvarchar(max) | N | The list of tables if supplied as part of an exclusion list. Note this can include wildcards |
| table_type | nvarchar(100) | N | Ranger value of type Inclusion or Exclusion |
| table_names | nvarchar(max) | N | The derived comma-separated table names without wildcards.

ranger_policies_staging
-----------------------
This table is automatically populated and temporarily stores all policy information obtained from the ranger endpoints for a given scheduled run of the store policies application. This table is used to compare the checksum against the target ranger_policies table to determine whether there is a difference and if so the target policy table is updated. This is done as part of a merge statement.  

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| Name | nvarchar(100) | N | Policy name as obtained from Ranger |
| RepositoryName | nvarchar(2000) | Y | Primary key. Name of the service therefore it is recommended that each service name is unique across all the provided endpoints |   
| Resources | nvarchar(max) | N | The path which the permission has been granted on. Only applicable to HDFS policies.
| paths | nvarchar(max) | N | The path which the permission has been granted on. Ojnly applicable to Hive policies.
| permMapList | nvarchar(max) | N | The permaplist is the json array of permissions for a given resource as provided by ranger |
| Databases | nvarchar(max) | N | The list of databases this policy applies to as obtained from Ranger i.e. can include a wildcard |
| DB_Names | nvarchar(max) | N | The derived value of comma seperated database name. Where a wildcard was used this will contain the full database names extracte |
| isRecursive | nvarchar(200) | N | Whether the policy is recursive as obtained from Ranger |
| Service Type | nvarchar(100) | N | Hive or HDFS |
| Status | nvarchar(100) | N | Whether the policy is active. Values are either True or False |
| checksum | nvarchar(400) | N | The checksum is a derived field calculated by running Ranger supplied values through a SHA1 algo to obtain a hash. This has compared to determine whether the record has changed |
| tables | nvarchar(max) | N | The list of tables if supplied as part of an exclusion list. Note this can include wildcards |
| table_type | nvarchar(100) | N | Ranger value of type Inclusion or Exclusion |
| table_names | nvarchar(max) | N | The derived comma-separated table names without wildcards.


policy_transactions
-------------------
This table is auto-populated by the store policies routine and primarily stores the permissions to be set on ADLS by path. 
The status represents whether the transaction can be ignored (for example where no valid principals were found), or whether it is pending, in progress or complete.
For ADLS API/SDK information please see [the documentation](https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update)

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| policy_id | int | Y | The policy ID as obtained from ranger |
| storage_url | nvarchar(max) | N | | The ADLS storage URL e.g. https://account.dfs.core.windows.net/container/folder |
| adl_path | nvarchar(max) | N | The path as obtained from Hive when underlying path is queried for a given database or table, i.e. hdfs:// |
| trans_type | int | N | The type of transaction i.e. new policy, deleted policy, modification. See the full list below |
| trans_action | nvarchar(200) | N | The action method used in the API/SDK call, curently only setAccessControlRecursive
| trans_mode | nvarchar(200) | N | The mode used in the API/SDK call, currently only either modify (new or modified permissions) or remove
| acentry | nvarchar(max) | N | The access control entry as required by the API/SDK |
| date_entered | datetime | N | The timestamp when this transaction was captured |
| trans_status | nvarchar(20) | N | The status of the transaction. The lifecycle of a transaction is described below. | 
| trans_reason | nvarchar(max) | N | Reason, if any, for the current status |
| continuation_token | nvarchar(max) | N | The continuation token provided by the API/SDK after each batch of 2000 ACLs |
| last_updated | datetime | N | The timestamp when this transaction record was updated |
| all_principals_excluded | nvarchar(1) | An indicator to represent whether all principals in this transaction were excluded in which case the status changes to ignore |
| principals_excluded | nvarchar(max) | N | The principals excluded at the time this transaction was created based on the exclusions table |
| exclusion_list | nvarchar(max) | N | The exclusion list as found in the exclusion table |
| principals_included | nvarchar(max) | N | The principals that were remaining after excluded principals were removed |
| acl_count | int | N | The number of ACLs changed. This value is updated as each batch (of 2000) is applied |
| adl_permission_str | nvarchar(3) | N | The ADLS permission string in the format of rwx. "-" indicates no permission for that position |
| permission_json | nvarchar(max) | The permission string in json format |
| depends_on | int | N | TBD The transaction ID that this transaction depends on/must wait for before being applied. This is used for defining a hierarchy of permissions to be applied |

Policy change types:
1 - new policy
2 - deleted policy *
3 - modification: policy enabled
4 - modification: policy disabled *
5 - modification: remove principals *
6 - modification: add principals
7 - modification: remove accesses/permissions * 
8 - modification: add accesses
9 - modification: remove paths *
10 - modification: add paths 

Transaction status lifecycle:
Validation - Initial status which requires validation. Validation is typically business or optimisation logic such as rule of maximum. 
Pending - Passed validation, waiting to sent to the queue
De-queued - Read from the queue, awaiting processing or currently in progress.
InProgress -  After the first 2000 ACLs are applied the status will be marked in progress if there are more ACLs to be applied recursively
Done - The ACLs changes have been successfully applied.
Abort - Stop any currently running transactions for this policy ID. This normally happens if a change to policy is made while a transaction for the same policy is in progress, i.e. latest version of the policy should take preference and any currently running transaction should stop. Before the ACLs are applied and at every 2000 ACLs the transaction status will be checked. 
Aborted - Stopped.

recon_report
------------
This table stores the differences between ADLS and Ranger



