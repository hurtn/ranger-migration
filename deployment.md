## Infrastructure Requirements and Deployment Guide

This solution supports either managed identity or service principal based authentication and authorisation to set permissions on the data lake and access the SQL database. From an application perspective, this is determined by whether the SPNID and SPNSecret app cconfigurations settings exist or not. If not, the managed identity of the Function App will be used throughout the solution. Based on your setup the first step may not be required.

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
 -  Python version 3.7
 -  Function runtime version ~3
 -  Default storage account options
 -  Linux operating system
 -  Elastic Premium EP1 app service plan
 -  SKU & Size: 210 total ACU, 3.5 GB memory
 -  Enable App Insights 
3. Enable managed identity of the function app if you are not using the service principal.
  https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet#add-a-system-assigned-identity

3.5 If you require private networking then at this point you may need to upgrade your storage account from gen1 to gen2. Click on the configuration blade and click the upgrade button. This will allow you to create a private endpoint to the storage account for the function app running within the vnet.

4. __SQL Managed Instance Database__ to store the Ranger policies. Ideally use a seperate database to avoid conflicts. The following SKU is sufficient: 
General Purpose Standard-series (Gen 5) (256 GB, 4 vCores
5. If you wish to use the managed identity of the Function App as a database user then there a few additional steps in italics to consider:
  - *First ensure the SQL MI identity has read permissions on the AAD. See [the following documentation](https://docs.microsoft.com/en-gb/azure/azure-sql/database/authentication-aad-configure?tabs=azure-powershell#azure-ad-admin-with-a-server-in-sql-database)
  - *Next, set an AAD admin. Please see [the following documentation](https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?tabs=azure-powershell#provision-azure-ad-admin-sql-managed-instance)
  - Create a new database and using a user with sysadmin permissions, and ensure to __enable CDC for the database__. See [the following documentation](https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql?view=sql-server-ver15)
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

1. Clone this repo to your local environment or the Azure Cloud Shell environment (simpler)
2. Navigate to the ranger-migration directory and deploy the function app code using the commands below depending on the environment, note the necessary pre-requisites. 
3. Alternatively deploy the function using [VSCode](https://docs.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=python) but ensure to chose the advanced option to deploy in the Premium app service plan.

Local Prerequisites

 * `func` cli version 3
 * `az` cli - requires you to az login and then set the subscription e.g. az account set --subscription "your-subscription-guid"
 * `.NET Core SDK` version 3.1

    ```
    az login
    az account set --subscription "your-subscription-guid"
    func azure functionapp publish your-function-app-name
    ```

Cloudshell

   ```
    func azure functionapp publish your-function-app-name
   ```    

4. Configure the following app settings

One may configure these settings manually or chose to execute them via the cli commands below
-  DatabaseConnxStr: This is the connection string to the SQL database in item 3 above. The format is Driver={ODBC Driver 17 for SQL Server};Server=tcp:[server].database.windows.net,1433;Database=[database];Authentication=ActiveDirectoryMsi. This uses the managed identity of the Function App to authenticate against the database which requires configuration as described in Step 4 above. If you are using SQL auth then please use Uid=xxxxx;Pwd=xxxxx instead of the Authentication flag.
-  dbname: This is the name of the database created in step 3 above
-  dbschema: Database schema, usually dbo
-  basestorageendpoint: This is the filesystem endpoint of the target storage location e.g. https://[storage_accoount].dfs.core.windows.net/[container]
-  HiveDatabaseConnxStr: This is the connection string to the Hive metastore. Depending on the Hive database engine, either use the MS SQL Server connection string format as above or use the MySQL SQLAlchemy format e.g. mysql+pymysql://[user:password@FQDN_or_IP:port]/[databasename]?charset=utf8mb4
-  ScheduleStoreAppSetting: How frequently the Apply policies application will run in NCRONTAB expression format i.e. {second} {minute} {hour} {day} {month} {day-of-week} so every 5 minutes would be 0 */5 * * * *
-  ScheduleApplyAppSetting: How frequently the Apply policies application will run in NCRONTAB expression format i.e. {second} {minute} {hour} {day} {month} {day-of-week} so every 5 minutes would be 0 */5 * * * *
-  ScheduleInitialiseAppSetting: How frequently the intialise application will run. This is not really meant to run on a schedule but is designed to be run once therefore we set this schedule to an obscure time so that it hardly ever would run on a timer, however we set the app to run on startup in the function.json file. Additionally there is the initialiseSQL config parameter below which when set to 1 will run the DDL, and then when set to 0 will effectively do nothing.
-  ScheduleReconAppSetting: How frequently the Recon process will run. An axample may be every Saturday morning at 9.30 so that it can run over the weekend.
-  rangerusername: This can be used as a global username for all ranger stores. Alternatively specify the username for each endpoint in the ranger_endpoint table.
-  rnagerpassword: This can be used as a global apsword for all ranger stores. Alternatively specify the password for each endpoint in the ranger_endpoint table. 
-  SPNID: Only create this setting if Service Principal is to be used, otherwise the application will default to managed identity based auth. Use the Service principal client ID.
-  SPNSecret:  Service principal secret. Note this can be stored securely as a key vault value. Use the format @Microsoft.KeyVault(SecretUri=https://keyvaultname.vault.azure.net/secrets/spnsecret/id) Only create this setting if Service Principal based auth is to be used, otherwise the application will default to managed identity.
-  tenantID: Tenant ID. This is used when looking up user/group object IDs in AAD
-  AzureStorageQueuesConnectionString: This is the connection string to the storage queue where the work items are stored/retrieved using the function app binding
-  initialiseSQL: This configuration defines whether to run the SQL intialisation script. Should be set to 1 for the first time and then set to 0 after the database has been initialised. Can be set to 2 if you wish to erase all the operational data i.e. not configuration data but policy and transaction information. This should be used with caution as it will trigger a full resynchronisation of all policies. Not that you will need to restart the app once the configuration change is made.

Alternatively review and use the CLI commands below:
```
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings DatabaseConnxStr="Driver={ODBC Driver 17 for SQL Server};Server=tcp:dbservername.database.windows.net,3342;Database=dbname;Uid=username;Pwd=password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings ScheduleApplyAppSetting="0 */10 * * * *"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings ScheduleApplyAppSetting="0 */10 * * * *"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings ScheduleInitAppSetting="0 */30 * * * *"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings ScheduleReconAppSetting="0 30 9 * * Sat"

az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings dbname="dbname"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings dbschema="dbo"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings tenantID=""
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings basestorageendpoint="https://storageaccountname.dfs.core.windows.net/containername"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings stage="live"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings HiveDatabaseConnxStr="Driver={ODBC Driver 17 for SQL Server};Server=tcp:dbservername.database.windows.net,3342;Database=dbname;Uid=username;Pwd=password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings rangerusername=""
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings rangerpassword=""
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings SPNID=""
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings SPNSecret=""

az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings WEBSITE_CONTENTOVERVNET="1"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings WEBSITE_DNS_SERVER="168.63.129.16"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings WEBSITE_VNET_ROUTE_ALL="1"
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings AzureStorageQueuesConnectionString=""
az webapp config appsettings set --name functionappaname --resource-group resourcegroupname --settings initialiseSQL="1"

```

5.  Assign permissions to the __Service Principal__ or __Managed Identity__ to access the Database, KeyVault and Storage

    1. To access key vault, add an Access Policy so that the identity has permissions to get secrets
    2. Follow step 4 in the infrastructure requirements above to grant permissions to the database
    3. To the target storage account, add the Storage Blob Data Owner or custom role (provied in this repository which denies blob data access) to the identity

6.  __Database initialisation__. There is an initialise application which runs after each funcion app restart which will create all the tables and initialise CDC for the user specified in the connection string. If the tables exist this will simply catch the exceptions but will not drop any existing objects. 

7. Secure Networking Configuration

If you wish to use private networking then the following post configuraiton are required:
1. Follow [the following guide](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-vnet#create-a-virtual-network) to configure a VNet with two subnets, lock down the storage account and integrate the function app.
2. Ensure [the following](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-vnet#configure-your-function-app-settings) function appc config settings have been applied
3. Create private endpoint for the database in the default subnet.

8. Configure diagnostic logging to Log Analytics. Please see [the documentation](https://docs.microsoft.com/en-us/azure/azure-functions/functions-monitor-log-analytics) for more details.  
9. AAD directory permissions - in order to look up users and groups in the directory the following permissions need to be granted to either the service principal or managed identity
![image](https://user-images.githubusercontent.com/5063077/124998868-0a978700-e045-11eb-93f2-ce271fe24029.png)
Please use [the following Powershell](https://techcommunity.microsoft.com/t5/integrations-on-azure-blog/grant-graph-api-permission-to-managed-identity-object/ba-p/2792127) to grant Directory.Read.All to the managed identity of the function app.
10. Read through the [table reference guide](https://github.com/hurtn/ranger-migration/blob/master/initialise/README.md) to understand which tables are deployed as part of the initialise function and which require values to be configured, for example the ranger endpoints.

