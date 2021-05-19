import datetime
import logging
import urllib
import pyodbc
import pandas as pd
import pandas.io.common
from sqlalchemy import create_engine
from sqlalchemy import event
import sqlalchemy
import azure.functions as func
import requests,uuid
from requests.auth import HTTPBasicAuth
from time import perf_counter 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    #storePolicy()
   

def getPolicyChanges():
    try:
                # configure database params
            dbname = "policystore"
            dbschema = "dbo"
            connxstr="Driver={ODBC Driver 13 for SQL Server};Server=tcp:cenpolicystor.public.ab33566069d1.database.windows.net,3342;Database="+dbname+";Uid=saadmin;Pwd=Obv10us123456789;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"

            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
            get_ct_info = "select lsn_checkpoint, sys.fn_cdc_map_time_to_lsn('smallest greater than',lsn_checkpoint) from " + dbname + "." + dbschema + ".policy_ctl where id= (select max(id) from " + dbname + "." + dbschema + ".policy_ctl);"
            #print(get_ct_info)
            print("Getting control table information...")
            cursor.execute(get_ct_info)
            row = cursor.fetchone()
            lsn_checkpoint=None
            policy_rows_changed=2
            if row:
                print("Last checkpoint was at "+str(row[0]))
                lsn_checkpoint  = row[0]
                next_lsn = row[1]
            else: print("No control information, obtaining all changes...")

               
            changessql = "DECLARE  @from_lsn binary(10), @to_lsn binary(10); " 
            if lsn_checkpoint is not None:
              changessql = changessql + """SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','""" + str(lsn_checkpoint) + """')
                                        SET @to_lsn = sys.fn_cdc_get_max_lsn() """

            else: 
                   changessql = changessql + """SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');
                                               SET @to_lsn = sys.fn_cdc_get_max_lsn(); """
                   #cursor.execute("select sys.fn_cdc_get_min_lsn('dbo_ranger_policies'), sys.fn_cdc_get_max_lsn()")
                   #row = cursor.fetchone()
                   #start_lsn = row[0]
                   #end_lsn = row[1]
                   #cursor.cancel() 
            changessql = changessql + """            
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status] 
            from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all update old') 
            order by id,__$seqval,__$operation;"""

            #print(changessql)
            if lsn_checkpoint is not None and next_lsn is not None:
              changesdf= pandas.io.sql.read_sql(changessql, cnxn)
              if changesdf is None:
                print("No changes found. Exiting...")
                exit()
            else:
                print("No changes found. Exiting...")
                exit()
            #print(changesdf)

            # filter by new policies entries
            insertdf = changesdf[(changesdf['__$operation']==2)]
            print("\nNew policy rows to apply:")
            print(insertdf)
            print("\n")
            acl_change_counter = 0
            if not insertdf.empty:
                #there are changes to process. first obtain an AAD token
                storagetoken = getBearerToken("storage.azure.com")
                graphtoken = getBearerToken("graph.microsoft.com")
                for row in insertdf.loc[:, ['Resources','Groups','Users','Accesses']].itertuples():
                    permstr=''
                    perms = row.Accesses.split(",")
                    for perm in perms:
                        if perm.strip() == 'read': permstr='r'
                        elif perm.strip() == 'write': permstr+='w'
                        elif perm.strip() == 'execute': permstr+='x'
                        else: permstr+='-'

                    if row.Users is not None and len(row.Users)>0:
                        userentries = row.Users.split(",")
                        for userentry in userentries:
                            #print("user: "+userentry.strip())
                            hdfsentries = row.Resources.strip("path=[").strip("]").split(",")
                            for hdfsentry in hdfsentries:
                                #print("path: "+hdfsentry.strip())
                                spnid = getSPID(graphtoken,userentry.strip(),'users')
                                acl_change_counter += setADLSPermissions(storagetoken, spnid, hdfsentry.strip(), permstr,'user')
                                #removeADLSPermissions(storagetoken, spnid, hdfsentry.strip(), permstr,'user')
                    if row.Groups is not None and len(row.Groups)>0:
                        groupentries = row.Groups.split(",")
                        for groupentry in groupentries:
                            #print("user: "+userentry.strip())
                            hdfsentries = row.Resources.strip("path=[").strip("]").split(",")
                            for hdfsentry in hdfsentries:
                                #print("path: "+hdfsentry.strip())
                                spnid = getSPID(graphtoken,groupentry.strip(),'groups')
                                acl_change_counter += setADLSPermissions(storagetoken, spnid, hdfsentry.strip(), permstr,'group')
                                #removeADLSPermissions(storagetoken, spnid, hdfsentry.strip(), permstr,'user')


            ## updates
            updatesdf = changesdf[(changesdf['__$operation']==3) |(changesdf['__$operation']==4)]
            print("\nUpdated policy rows to process:")
            print(updatesdf)
            print("\n")
            rowid = 0
            for index, row in updatesdf.iterrows():
              if rowid != row['id']:
                #reset the arrays per unique policy ID
                groupsafter = []
                groupsbefore = []
                usersbefore = []
                usersafter = []
                resourcesbefore = []
                resourcesafter = []
                accessesbefore = []
                accessesafter = []
                statusbefore = ''
                statusafter = ''
                print("policy id "+str(row['id']))
                rowid = row['id']
                # fetch the first and last row of changes for a particular ID. This is because we are only concerned with the before 
                # and after snapshot,even if multiple changes took place
                firstandlastforid = updatesdf[(updatesdf['id']==rowid)].iloc[[0, -1]]
                # don't be confused by the for loops below - 
                # it must seem a really odd way to get the row from the pandas series but I couldn't find another elegant way yet. 
                # essentially this is just fetching the one row from each iterrows to store the before and after value from the iloc 0,-1 filter above
                for index,row in firstandlastforid.iloc[[0]].iterrows():
                  if row.Groups: groupsbefore = row.Groups.split(",")
                  resourcesbefore = row.Resources.strip("path=[").strip("]").split(",")
                  if row.Users: usersbefore = row.Users.split(",")
                  accessesbefore = row.Accesses.split(",")
                  statusbefore = row.Status
                for index,row in firstandlastforid.iloc[[1]].iterrows():
                  if row.Groups: groupsafter = row.Groups.split(",")
                  resourcesafter = row.Resources.strip("path=[").strip("]").split(",")
                  if row.Users: usersafter = row.Users.split(",")
                  accessesafter = row.Accesses.split(",")
                  statusafter = row.Status

                def entitiesToAdd(beforelist, afterlist):
                    return (list(set(afterlist) - set(beforelist)))

                def entitiesToRemove(beforelist, afterlist):
                    return (list(set(beforelist) - set(afterlist)))                    

                # determine group changes
                addgroups = entitiesToAdd(groupsbefore,groupsafter)
                if addgroups: 
                    print("add the following groups")
                    for grouptoadd in addgroups:
                        print(grouptoadd)

                removegroups = entitiesToRemove(groupsbefore,groupsafter)    
                if removegroups:
                    print("remove the following groups")
                    for grouptoremove in removegroups:
                        print(grouptoremove)
                
                # determine user changes
                addusers = entitiesToAdd(usersbefore,usersafter)
                if addusers:
                    print("add the following users")
                    for usertoadd in addusers:
                        print(usertoadd)

                removeusers = entitiesToRemove(usersbefore,usersafter)    
                if removeusers:
                    print("remove the following users")
                    for usertoremove in removeusers:
                        print(usertoremove)

                # determine access changes
                addaccesses = entitiesToAdd(accessesbefore,accessesafter)
                if addaccesses:
                    print("add the following accesses")
                    for accesstoadd in addaccesses:
                        print(accesstoadd)

                removeaccesses = entitiesToRemove(accessesbefore,accessesafter)    
                if removeaccesses:
                    print("remove the following accesses")
                    for accesstoremove in removeaccesses:
                        print(accesstoremove)

                if statusbefore != statusafter:
                    if statusafter == 'Enabled': print('Policy now enabled')    
                    if statusafter == 'Disabled': print('Policy now disabled')    

                 # determine access changes   
                addresources = entitiesToAdd(resourcesbefore,resourcesafter)
                if addresources:
                    print("add the following resources")
                    for resourcetoadd in addresources:
                        print(resourcetoadd)
                removeresources = entitiesToRemove(resourcesbefore,resourcesafter)    
                if removeresources:
                    print("remove the following resources")
                    for resourcetoremove in removeresources:
                        print(resourcetoremove)


            acl_change_counter = 0

            # save checkpoint in control table
            set_ct_info = "insert into " + dbname + "." + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('applyPolicies', current_timestamp,'" + formatted_date + "','"+ formatted_date + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
            #print(set_ct_info)
            cursor.execute(set_ct_info)

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            print('Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            print('Successfully processed file!')
    finally:
            cnxn.autocommit = True



def setADLSPermissions(aadtoken, spn, adlpath, permissions, spntype):
    basestorageuri = 'https://baselake.dfs.core.windows.net/base'
    spnaccsuffix = ''
    #print(spn + '-' + adlpath)
    # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    #Setup the endpoint
    puuid = str(uuid.uuid4())
    #print('Log analytics UUID'+ puuid)
    headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype+':'+spn+spnaccsuffix + ':'+permissions+',default:'+spntype+':'+spn+spnaccsuffix + ':'+permissions,'x-ms-client-request-id': '%s' % puuid}
    #headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype+':'+spn+spnaccsuffix +',default:'+spntype+':'+spn+spnaccsuffix ,'x-ms-client-request-id': '%s' % puuid}
    request_path = basestorageuri+adlpath+"?action=setAccessControlRecursive&mode=modify"
    print("Setting " + permissions + " ACLs for " + spntype + " " + spn + " on " +adlpath + "...")
    t1_start = perf_counter() 
    r = requests.patch(request_path, headers=headers)
    response = r.json()
    t1_stop = perf_counter()
    #print(r.text)
    print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
    return(response["filesSuccessful"])

def removeADLSPermissions(aadtoken, spn, adlpath, permissions, spntype):
    basestorageuri = 'https://baselake.dfs.core.windows.net/base'
    spnaccsuffix = ''
    #print(spn + '-' + adlpath)
    # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    #Setup the endpoint
    puuid = str(uuid.uuid4())
    #print('Log analytics UUID'+ puuid)
    headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype+':'+spn+spnaccsuffix +',default:'+spntype+':'+spn+spnaccsuffix ,'x-ms-client-request-id': '%s' % puuid}
    request_path = basestorageuri+adlpath+"?action=setAccessControlRecursive&mode=remove"
    print("Removing " + permissions + " ACLs for " + spntype + " " + spn + " on " +adlpath + "...")
    t1_start = perf_counter() 
    r = requests.patch(request_path, headers=headers)
    response = r.json()
    t1_stop = perf_counter()
    #print(r.text)
    print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  


def getSPID(aadtoken, spn, spntype):
    if spntype == 'users': odatafilterfield = "userPrincipalName"
    else: odatafilterfield = "displayName"
    print("Tenant look up for " + spntype + ": " + spn )
    headers ={'Content-Type': 'application/json','Authorization': 'Bearer %s' % aadtoken}
    request_str = "https://graph.microsoft.com/v1.0/"+spntype+"?$filter=startswith("+odatafilterfield+",'"+spn.replace('#','%23')+"')"
    #https://graph.microsoft.com/v1.0/users?$filter=startswith(userPrincipalName,'nihurt@microsoft.com')
    #print(aadtoken)
    #print(request_str)
    r = requests.get(request_str, headers=headers)
    response = r.json()
    print("Found OID " + response["value"][0]["id"])
    return response["value"][0]["id"]
    

def getBearerToken(resourcetype):
    endpoint = 'https://login.microsoftonline.com/af26513a-fe59-4005-967d-bd744f659830/oauth2/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'grant_type=client_credentials&client_id=a86005a7-2865-4db4-8c0f-305247a0544e&client_secret=z0BYa3~7~qjY~Qfg7Q2.1_U~D3Hgb0vu~z&resource=https%3A%2F%2F'+resourcetype+'%2F'
    r = requests.post(endpoint, headers=headers, data=payload)
    response = r.json()
    print("Obtaining AAD bearer token for resource "+ resourcetype + "...")
    #print(response)
    bearertoken = response["access_token"]
    #print(bearertoken)
    print("Bearer token obtained.\n")
    return bearertoken


getPolicyChanges()



