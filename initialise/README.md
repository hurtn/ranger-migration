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
This table is used to store policy or principal exclusions. For policies the exact policy name and matching case should by provided to exclude policies with a given a name. For users provided the user principal name, and for groups provide the security group name. For users and groups, case is ignored.   

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int | - | Row identifier and primary key |
| type | nvarchar(1) | Y | The type of exclusion. Possible values are "P" for policy or "U" for user or "G" for group.
| identifier | nvarchar(100) | Y | The name of either the policy, user principal name, or group. |
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |


perm_mapping
------------

This table stores the mapping of Ranger permissions to ADLS equivalent permissions. Needs to be populated prior to running the application.

| Column name |  Column Type | Required | Description |
|-------------|--------------|----------|-------------|
| ID | int |- | Row identifier and primary key |
| ranger_perm | nvarchar(100) | Y | The ranger permission value e.g. select, read, update, write, execute |
| adls_perm | nvarchar(1) | Y | The ADLS equivalent permission. Possible values are "r", "w" or "x" |
| date_entered | datetime | N | Optional: Enter a date when the mapping was created |
| entered_by | nvarchar(100) | N | Optional: The user who captured this mapping. |

Here are a sample set of inserts:

insert into perm_mapping (ranger_perm, adls_perm) values ('select','r')           
insert into perm_mapping (ranger_perm, adls_perm) values ('read','r')           
insert into perm_mapping (ranger_perm, adls_perm) values ('update','w')           
insert into perm_mapping (ranger_perm, adls_perm) values ('write','w')           
insert into perm_mapping (ranger_perm, adls_perm) values ('execute','x')   

