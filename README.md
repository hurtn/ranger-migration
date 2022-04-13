<img align="left" width="80" height="75" src="https://github.com/hurtn/ranger-migration/blob/master/images/RaPTor.png" alt="RaPTor icon"/>

# Project RaPTor - Ranger Policy Translator Tool

&nbsp;

## Introduction 

The intended purpose of this tool is to periodically synchronise resource based Apache Ranger<sup>TM</sup> policies with Azure Datalake Storage (ADLS) ACLs. 
There are three main Python applications in this repo which support this:
1. storePolicies: retrieve policies from one or more Ranger policy stores and store these in a SQL database table
2. applyPolicies: read changes from the policy table (using the CDC API) and create, validate and queue transactions (work items) which encapsulate the permissions to be applied as Storage ACLs
3. aclWorkers: process work items from the queue by modifying or removing ACLs on ADLS

Other supporting applications:
1. policyRecon: periodically performs a recon between expected ACLs and actual ACLs
2. initialise: first initialisation of the database tables and CDC
3. reprocessFailed: removes all failed work items from the poison queue and resets these items in the database to be (requeued and) retried

### High Level Architecture
&nbsp;
![image](https://user-images.githubusercontent.com/5063077/145583409-dc359f85-8ce7-4918-9bab-4f95affc9b5d.png)
&nbsp;

#### Summary of Features and Functionality
1. Retrieve and store policies from multiple Ranger policy stores
2. Look up database location in the Hive metastore database and store it with the associated policy entry
3. Ignore specific policies using exclusions table
4. Supports table level exclusions
5. Determine policy changes and incrementally apply policy updates (using storage ACLs) supporting the following scenarios
  - New policies
  - Deleted/disabled policies
  - Policy amendments
    - Adding/removing new databases
    - Adding/removing users or groups
    - Added/removing permissions
  - Ignoring principals (groups or users) based on exclusion table
  - ACLs are set recursively (including defaults) using the modify operation to ensure that we preserve existing ACLs. See [the docs](https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update) for more info.
 6. Independent audit process which extracts ACLs for all paths in Ranger policies and compares them to the expected set of ACLs. Inconsistencies are stored in a table which can be regularly polled by Logic App process or PBI report to send alerts.


## Deployment details
Please see the [following deployment guide](https://github.com/hurtn/ranger-migration/blob/master/deployment.md)

## Under the hood

Technical details of the process flow within each app of this project is explained below.

- storePolicies process flow: store Hive Policies

![image](https://user-images.githubusercontent.com/5063077/128572674-165cebf4-6e61-4b0b-ab85-89f28be4f49e.png)

- applyPolicies process flow: apply policy changes

![image](https://user-images.githubusercontent.com/5063077/118631114-185c2900-b7c7-11eb-9dda-c92fcef405a3.png)

- Process flow for modified policies

![image](https://user-images.githubusercontent.com/5063077/128572798-d69d3b24-8d6d-4ab6-95d5-29118044797b.png)

## Latest Improvements

- Reconcilliation process to periodically audit actual storage ACLs vs expected permissions found in Ranger
- Exclude principals and policies from the sychronisatoin process
- ACLs are applied [concurrently](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-queue-trigger?tabs=csharp#concurrency) (scale out via in-built storage queue algorithm) and applied asynchronously.
- Validation process examines whether other policies are in conflict with the requested change and if no the request is modified or ignored (rule of maximum)
- Enhanced logging and metrics are available via the transactions table to keep track of all transaction statuses, progress (number of ACLs updated) and continuation tokens
- Long running processes can be interrupted/aborted if the same policy is updated with other changes whilst a transaction is in progress. 
- Failed processes can recover from where the point the transaction was interrupted (within the last batch of 2000) using the continuation token provided by the SDK.
- Recorded demos of mutiple test cases including adding and removing of permissions in Ranger to one or more policies and associated databases. Please see [the videos folder](https://github.com/hurtn/ranger-migration/tree/master/videos)
- Instead of making one set ACL API call per user or group, we can batch this is into one call per directory and permission set by using a comma separated lists of access control entries (ACE)
- No JAR based hive driver is required to connect to hive - the existing pyodbc driver is used and queries are made directly against the Hive database. This reduces the number of dependencies (JVM and JPype) and makes the application bunder much smaller as the Hive Jar was >100MB.
- Utilise the permMapList array to cater for multiple permissions, users and groups assigned to a single policy

## Immediate TODOs
- add an exceptions list for users and groups to be ignore in the sync process (these are most likely service accounts or non AAD identites)

## Potential Future enhancements
- Whilst multiple ranger sources are supported via a comma separated string in the configuration parameter hdiclusters, it only supports one Hive databases connection string currently.
- Support for non CDC enabled Databases by using before and after snapshot tables to determine changes instead of CDC. Currently only SQL DB tiers which support CDC can be used e.g. serverless option requires 2vcpus min.
- Policy Synchronisation Validation
  - Periodically polls and reports on Ranger and ACLs for discrepancies
  - Option to “force” re-sync of all policy defs (full re-sync vs incremental). Note the danger of this apporoach is if ACLs are set outside of this process for other non Ranger users hence the modify mode over set mode was used to prevent inadvertantly overwriting existing ACLs set outside of the sync process
- Storage ACL report in Power BI

## Current limitations
- Only supports HDFS & Hive service type
- Only support resource based policies in ranger, does not support attribute / tag based policies
- Does not support deny or exclude policies

## Known issues
- The CDC logic intermittently fails with "An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_". This is due to the way in which SQL Server returns information about the validity of the lsn ranges i.e. there is no way to distinguish between an LSN which is out of bounds or no change capture information. See the following [documentation](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-data-sql-server?view=sql-server-ver15#LSN. This error should most likely be trapped and handled accordingly)

