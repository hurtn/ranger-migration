### Ranger Migration Synchronisation App

There are two python applications in this repo which:
1. storePolicies: read Policies from a csv file (future capability to read from Ranger API), store in a CDC enabled table in SQL
2. applyPolicies: read changes from the SQL table and apply the permissions to Storage ACLs

![image](https://user-images.githubusercontent.com/5063077/118630985-fa8ec400-b7c6-11eb-9831-5dcaabbf8ab4.png)

### storePolicies process flow: store HDFS Policies

![image](https://user-images.githubusercontent.com/5063077/118631057-0d08fd80-b7c7-11eb-9626-0ed6259bfd96.png)

### applyPolicies process flow: apply policy changes

![image](https://user-images.githubusercontent.com/5063077/118631114-185c2900-b7c7-11eb-9dda-c92fcef405a3.png)

### Future enhancements
- Investigate whether multiple allow conditions need to be merged into the same policy record
- Cater for multiple ranger sources (determine identifiers for unique and context awareness / potentially priotisation)
- Cater for user/groups exceptions list
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



