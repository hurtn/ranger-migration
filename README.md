### Ranger Migration Synchronisation App

There are two python applications in this repo which:
1. storePolicies: read Policies from a csv file (future capability to read from Ranger API), store in a CDC enabled table in SQL
2. applyPolicies: read changes from the SQL table and apply the permissions to Storage ACLs

![image](https://user-images.githubusercontent.com/5063077/118630985-fa8ec400-b7c6-11eb-9831-5dcaabbf8ab4.png)

### storePolicies process flow: store HDFS Policies

![image](https://user-images.githubusercontent.com/5063077/118631057-0d08fd80-b7c7-11eb-9626-0ed6259bfd96.png)

### applyPolicies process flow: apply policy changes

![image](https://user-images.githubusercontent.com/5063077/118631114-185c2900-b7c7-11eb-9dda-c92fcef405a3.png)

## Setup of local environment
- Create a service principal
- Ensure access to a SQL MI instance or SQL DB (over 2 vcores to support CDC)
- Store database connection string as an environment variable with the following keyvalue pair and use the jdbc connection format e.g. "DatabaseConnxStr":"Driver={ODBC Driver 13 for SQL Server};Server=tcp:[dbendpointprefix].database.windows.net,3342;Database=[dbname];Uid=[username];Pwd=[password];Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
- Create a database and run the setup script
- Populate some entries in the NHPolicySample csv which match the paths in your storage location, users and groups in your tenant.

## Current limitations
- Only supports HDFS service type
- Only support resource based policies in ranger, does not support attribute / tag based policies

## Latest Improvements
- Instead of making one set ACL API call per user or group, we can batch this is into one call per directory and permission set by using a comma separated lists of access control entries (ACE)
- Convert the ACL API call to Powershell or SDK and make async when moving to durable functions
- Implement Hive support which will query hive to determine the underlying table path
- Set the base storage path as an App config setting

### Immediate TODOs
- validate and enhance the graph OID lookup routine which use the Odata filter is robust enough and can handle lookup errors or zero hits
- convert the API call to Async

### Future enhancements
- Investigate whether multiple allow conditions need to be merged into the same policy record
- Cater for multiple ranger sources (determine identifiers for unique and context awareness / potentially priotisation)
- Cater for database, users, groups exceptions list
- Cater for Hive policies
- Implementation of control table and process run reporting
- Investigation into SQL DB rather SQL MI compatibility
- Improve scalability (if required):
  - Migrate app to durable functions
  - Convert ACL API call to asynchronous call
- Policy Synchronisation Validation
  - Periodically polls and reports on Ranger and ACLs for discrepancies
  - Option to “force” re-sync of all policy defs (full re-sync vs incremental)
- Storage ACL report in Power BI



