This section describes the various tables and columns, their purpose and possible values.

policy_ctl
----------
This table is primarily used to keep track of database LSN (logical sequence number) timestamp of each scheduled run. This is used to determine the set of changes since the last time the function app ran. Other uses include store metrics and information associated with each run.

| Column name |  Column Type | Description |
|-------------|--------------|-------------|
| ID | int | Row identifier and primary key |
| application | NVARCHAR(30) | Name of the application e.g. storePlicies, applyPolicies |
| start_run | datetime | Start timestamp of the run |
| end_run | datetime | End timestamp of the run | 
| lsn_checkpoint | datetime | Timestamp of the last run |
| rows_changed | int | The number of rows merged in the policy table. Applicable only to the storePolicies application which loads the policies from Ranger and stores them in the policy table |
| ACLs_changed | int | The total number of ACLs changed during each run. Applicable only to the applyPolicies application which converts the policies into transactions, applies validation and business logic before sending transactions to the queue for processing |
