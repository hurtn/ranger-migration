### Ranger Migration Synchronisation App

The intended purpose of these applications is to periodically synchronise resource based Ranger policies with ADLS storage ACLs. There are two Python applications in this repo which:
1. storePolicies: read Policies from a csv file (future capability to read from Ranger API), store in a CDC enabled table in SQL
2. applyPolicies: read changes from the SQL table and apply the permissions to Storage ACLs

![image](https://user-images.githubusercontent.com/5063077/128572626-1d1378bf-eafb-4a5a-a470-dbfab9f727b6.png)

### storePolicies process flow: store Hive Policies

![image](https://user-images.githubusercontent.com/5063077/128572674-165cebf4-6e61-4b0b-ab85-89f28be4f49e.png)

### applyPolicies process flow: apply policy changes

![image](https://user-images.githubusercontent.com/5063077/118631114-185c2900-b7c7-11eb-9dda-c92fcef405a3.png)

### Process flow for modified policies

![image](https://user-images.githubusercontent.com/5063077/128572798-d69d3b24-8d6d-4ab6-95d5-29118044797b.png)

## Deployment details
Please see the [following deployment guide](https://github.com/hurtn/ranger-migration/blob/master/deployment.md)

- If developing locally, store the service principal client ID and secret as environment variables using keys spnid and spnsecret. Note you may need to restart your pc for these to take effect.
- Ensure access to a SQL MI instance or SQL DB (serverless or GP with over 2 vcores to support CDC - this is currently a preview capability)
- Store database connection string as an environment variable with the following keyvalue pair and use the jdbc connection format e.g. "DatabaseConnxStr":"Driver={ODBC Driver 13 for SQL Server};Server=tcp:[dbendpointprefix].database.windows.net,3342;Database=[dbname];Uid=[username];Pwd=[password];Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;" or if using managed identity of the function app use Authentication=ActiveDirectoryMsi instead of UID and pwd.

- Create a database and run the setup script
- Populate some entries in the NHPolicySample csv which match the paths in your storage location, users and groups in your tenant.

## Current limitations
- Only supports HDFS & Hive service type
- Only support resource based policies in ranger, does not support attribute / tag based policies
- Does not support deny or exclude policies

## Known issues
- The CDC logic intermittently fails with "An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_". This is due to the way in which SQL Server returns information about the validity of the lsn ranges i.e. there is no way to distinguish between an LSN which is out of bounds or no change capture information. See the following [documentation](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-data-sql-server?view=sql-server-ver15#LSN. This error should most likely be trapped and handled accordingly)

## Latest Improvements
- Instead of making one set ACL API call per user or group, we can batch this is into one call per directory and permission set by using a comma separated lists of access control entries (ACE)
- No JAR based hive driver is required to connect to hive - the existing pyodbc driver is used and queries are made directly against the Hive database. This reduces the number of dependencies (JVM and JPype) and makes the application bunder much smaller as the Hive Jar was >100MB.
- Utilise the permMapList array to cater for multiple permissions, users and groups assigned to a single policy
- Recorded video demos of mutiple test cases including adding and removing of permissions in Ranger to one or more policies and associated databases. Check out the videos folder!

### Immediate TODOs
- validate that the Microsoft Graph OID lookup routine which use an Odata startswith filter is robust enough in a representative directory
- add an exceptions list for users and groups to be ignore in the sync process (these are most likely service accounts or non AAD identites)

### Potential TODOs
- If converting to durable functions, convert the ACL API call to Powershell or SDK and make async when moving to durable functions
- current only the prior image of the record and the latest image of the record are used to determine changes (see comment 11b in the applyPolicies code), i.e. whilst we could potentially capture intermediate changes through the CDC framework we effectively ignore them e.g. multiple/intermediate updates to the same record, we need to evaluate whether this has any negative consequence as we could miss a change in a complex scenario e.g. trying to add and remove multiple groups/users, with a change in permission set in one go rather than "walking" the timeline of changes. As we may not be capturing every single change in this manner due to the way in which this is a period "pull" rather than push, the store process may not get this level of detail in the capture records. **Currently if such a complex scenario arises, removal of users/groups is handled as a recursive deletion of these users/groups from the ACLs from the previous image of the path (in case this was part of the changed fields), and then the new users/groups are recursively added to the latest image of the path. Note all modifications use the modify operation as opposed to the set operation because set replaces all previous entries of that ACL and there could be ACEs set outside of this process which we do not want to destroy, there using the modify operation ensures that we preserve existing ACLs. See [the docs](https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update) for more info.**


### Future enhancements
- Whilst multiple ranger sources are supported via a comma separated string in the configuration parameter hdiclusters, an additional feature is required to map these to multiple Hive databases and their respective connection string.
- Support for non CDC enabled Databases by using before and after snapshot tables to determine changes instead of CDC.
- Policy Synchronisation Validation
  - Periodically polls and reports on Ranger and ACLs for discrepancies
  - Option to “force” re-sync of all policy defs (full re-sync vs incremental). Note the danger of this apporoach is if ACLs are set outside of this process for other non Ranger users hence the modify mode over set mode was used to prevent inadvertantly overwriting existing ACLs set outside of the sync process
- Storage ACL report in Power BI



