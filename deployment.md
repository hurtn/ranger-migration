## Infrastructure Requirements

If the managed identity of the Function App is to be used throughout the solution then skip the first step, otherwise create a service principal to set permissions on the data lake and access the SQL database.

 1. __Service Principal__.   
 
    ```
    az ad sp create-for-rbac --name <fn_name>
    ```

    *NOTE: record the output, you will need the __appId__, __password__, and __tenant__ later.
    
        ```
        {
        "appId": "xxxx",
        "displayName": "xxxx",
        "name": "xxxx",
        "password": "xxxx",
        "tenant": "xxxx"
        }
        ```

2. __Function App__ with the following configuration:
 -  Python version 3.6
 -  Function runtime version ~3
 -  Default storage account options
 -  Linux operating system
 -  Premium app service plan
 -  Enable App Insights 
3. Enable managed identity of the function app if you are not using the service principal.
  https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet#add-a-system-assigned-identity

3.5 If you require private networking then at this point you may need to upgrade your storage account from gen1 to gen2. Click on the configuration blade and click the upgrade button. This will allow you to create private endpoints to the storage account for the function app running within the vnet.

4. __SQL Managed Instance Database__ to store the Ranger policies. Ideally use a seperate database to avoid conflicts.
5. If you wish to use the managed identity of the Function App as a database user then there a few additional steps in italics to consider:
  - *First ensure the SQL MI identity has read permissions on the AAD. See [the following documentation](https://docs.microsoft.com/en-gb/azure/azure-sql/database/authentication-aad-configure?tabs=azure-powershell#azure-ad-admin-with-a-server-in-sql-database)
  - *Next, set an AAD admin. Please see [the following documentation](https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?tabs=azure-powershell#provision-azure-ad-admin-sql-managed-instance)
  - Create a new database and using a user with sysadmin permissions, enable CDC for the database. See [the following documentation](https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql?view=sql-server-ver15)
  - If necessary create a separate database user and provide the appropriate permissions.
  - *Create the user and provide necessary permissions e.g.:
      - CREATE USER [Function App name] FROM EXTERNAL PROVIDER;
      - GRANT CONTROL ON DATABASE::[centricapolicydb] TO [Function App name];
   - Ensure the database is initialised using the initialise function application (see notes below) using this above user in the connection string details. This will ensure that CDC is enabled to the policy table for the correct user.

6. Target __ADLS storage account__ where the ACLs will be applied (may exist already)
7. Ranger and Hive services, usually deployed as part of __HDInsight__.
  - an AAD user/service account with appropriate priviledges (e.g. admin role on the clusters) to authenticate against the Ranger API and Hive database.
8. Optional: deploy a jumpbox VM inside the VNet to faciliate Database or Storage Account configuration once these services have been locked down.

## Configure the Function App

1. Clone this repo to your local environment
2. Navigate to the ranger-migration directory and deploy the function app code using the command below with the necessary pre-requisites. 
3. Alternatively deploy the function using [VSCode](https://docs.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=python) but ensure to chose the advanced option to deploy in the Premium app service plan.

Local Prerequisites

 * `func` cli version 3
 * `az` cli - requires you to az login and then set the subscription e.g. az account set --subscription "your-subscription-guid"
 * `.NET Core SDK` version 3.1

    ```
    az login
    az account set --subscription "your-subscription-guiid"
    func azure functionapp publish your-function-app-name
    ```

4. Configure the following app settings
-  DatabaseConnxStr: This is the connection string to the SQL database in item 3 above. The format is Driver={ODBC Driver 17 for SQL Server};Server=tcp:[server].database.windows.net,1433;Database=[database];Authentication=ActiveDirectoryMsi. This uses the managed identity of the Function App to authenticate against the database which requires configuration as described in Step 4 above. If you are using SQL auth then please use Uid=xxxxx;Pwd=xxxxx instead of the Authentication flag.
-  dbname: This is the name of the database created in step 3 above
-  dbschema: Database schema, usually dbo
-  hdiclusters: comma separated list of server names which will be used to extract the policies via the Ranger API. To this name "-int.azurehdinsight.net" is added to complete the endpoint details.
-  basestorageendpoint: This is the filesystem endpoint of the target storage location e.g. https://[storage_accoount].dfs.core.windows.net/[container]
-  HiveDatabaseConnxStr: This is the connection string to the Hive metastore using SQLAlchemy format e.g. user:password@FQDN_or_IP:port
-  ScheduleStoreAppSetting: How frequently the Apply policies application will run in NCRONTAB expression format i.e. {second} {minute} {hour} {day} {month} {day-of-week} so every 5 minutes would be 0 */5 * * * *
-  ScheduleApplyAppSetting: How frequently the Apply policies application will run in NCRONTAB expression format i.e. {second} {minute} {hour} {day} {month} {day-of-week} so every 5 minutes would be 0 */5 * * * *
-  SPNID: Service principal client ID (only required if using a service principal vs Fn app identity to set ACL permissions)
-  SPNSecret: Service principal secret. Note this can be stored securely as a key vault value. Use the format @Microsoft.KeyVault(SecretUri=https://keyvaultname.vault.azure.net/secrets/spnsecret/id)
-  tenantID: Tenant ID. This is used when looking up user/group object IDs in AAD

5.  Assign permissions to the __Service Principal__ or __Managed Identity__ to access the Database, KeyVault and Storage

    1. To access key vault, add an Access Policy so that the identity has permissions to get secrets
    2. Follow step 4 in the infrastructure requirements above to grant permissions to the database
    3. To the target storage account, add the Storage Blob Data Owner or custom role (provied in this repository which denies blob data access) to the identity

6.  __Database initialisation__. There is an initialise application which runs after each funcion app restart which will create all the tables and initialise CDC for the user specified in the connection string. If the tables exist this will simply catch the exceptions but will not drop any existing objects. 

7. Secure Networking Configuration

If you wish to use private networking then the following post configuraiton are required:
1. Follow [the following guide](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-vnet#create-a-virtual-network) to configure a VNet with two subnets, lock down the storage account and integrate the function app.
2. Ensure [the following](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-vnet#configure-your-function-app-settings) function appc config settings have been applied
3. Create private endpoints for the database in the default subnet.

8. Configure diagnostic logging to Log Analytics. Please see [the documentation](https://docs.microsoft.com/en-us/azure/azure-functions/functions-monitor-log-analytics) for more details.  
9. AAD directory permissions - in order to look up users and groups in the directory the following permissions need to be granted to either the service principal or managed identity
![image](https://user-images.githubusercontent.com/5063077/124998868-0a978700-e045-11eb-93f2-ce271fe24029.png)

