import os
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
from collections import defaultdict
import json

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    getPolicyChanges()
   

def getPolicyChanges():
    def getPermSeq(perms):
        for perm in perms:
            if perm.strip() == 'read': permstr='r'
            elif perm.strip() == 'select': permstr='r'
            elif perm.strip() == 'write': permstr+='w'
            elif perm.strip() == 'update': permstr+='w'
            elif perm.strip() == 'execute': permstr+='x'
            elif perm.strip() == 'all': return 'rwx'
            else: permstr+='-'
        return permstr

    def getSPID(aadtoken, spname, spntype):
        # Graph docs - Odata filter: https://docs.microsoft.com/en-us/graph/query-parameters#filter-parameter
        if spntype == 'users': odatafilterfield = "userPrincipalName"
        else: odatafilterfield = "displayName"
        spname = spname.strip().strip("'") #cleanup
        print("AAD Directory look up for " + spntype + ": " + spname)
        headers ={'Content-Type': 'application/json','Authorization': 'Bearer %s' % aadtoken}
        request_str = "https://graph.microsoft.com/v1.0/"+spntype+"?$filter=startsWith("+odatafilterfield+",'"+spname.strip().replace('#','%23')+"')"
        #https://graph.microsoft.com/v1.0/users?$filter=startswith(userPrincipalName,'nihurt@microsoft.com')
        #print(aadtoken)
        #print(request_str)
        r = requests.get(request_str, headers=headers)
        if r.status_code==200:
            response = r.json()
            #print(response)
            if response["value"]:
              print("Found OID " + response["value"][0]["id"])
              return response["value"][0]["id"]
            else:
              print("Warning: Could not find user ID!!!")
              # at this point should we aboort the process or just log the failure?? TBD by client
              return None
        else:
            print("Warning: Could not find user ID!!!")
            # at this point should we aboort the process or just log the failure?? TBD by client
            return None

    def getSPIDs(userslist, groupslist):
        spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL

        # iterate through the comma separate list of groups and set the dictionary object
        if userslist is not None and len(userslist)>0:
            userentries = str(userslist).split(",")
            for userentry in userentries:
                #print("user: "+userentry.strip("['").strip("']").strip(' '))
                spnid = getSPID(graphtoken,userentry.strip("['").strip("']").strip("'").strip(' '),'users')
                if spnid is not None:
                  spids['user'].append(spnid)

        # iterate through the comma separate list of groups and set the dictionary object
        if groupslist is not None and len(groupslist)>0:
            groupentries = str(groupslist).split(",")
            for groupentry in groupentries:
                spnid = getSPID(graphtoken,groupentry.strip("['").strip("']").strip("'").strip(' '),'groups')
                if spnid is not None:
                  spids['group'].append(spnid)
        return spids
   
    # utility functions to determine differences in lists
    def entitiesToAdd(beforelist, afterlist):
        return (list(set(afterlist) - set(beforelist)))

    def entitiesToRemove(beforelist, afterlist):
        return (list(set(beforelist) - set(afterlist)))                    

    ## determine whether the lists are equal if sorted i.e. the same elements just in different order
    def check_if_equal(list_1, list_2):
        """ Check if both the lists are of same length and if yes then compare
        sorted versions of both the list to check if both of them are equal
        i.e. contain similar elements with same frequency. """
        if len(list_1) != len(list_2):
            return False
        return sorted(list_1) == sorted(list_2)


    try:
            # configure database params
            dbschema = "dbo"
            connxstr=os.environ["DatabaseConnxStr"]
            spnid= os.environ["SPNID"]
            spnsecret= os.environ["SPNSecret"]
            appname = 'applyPolicies'

            dbname = 'policystore'
            stagingtablenm = "ranger_policies_staging"
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            #collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
            get_ct_info = "select lsn_checkpoint from " + dbname + "." + dbschema + ".policy_ctl where id= (select max(id) from " + dbname + "." + dbschema + ".policy_ctl where application = '" + appname +"');"
            #print(get_ct_info)
            print("Getting control table information...")
            cursor.execute(get_ct_info)
            row = cursor.fetchone()
            lsn_checkpoint=None
            acl_change_counter = 0
            policy_rows_changed =0

            if row:
                print("Last checkpoint was at "+str(row[0]))
                lsn_checkpoint  = row[0]
            else: print("No control information, obtaining all changes...")

            # changessql is the string variable which holds the SQL statements to determine the changes since the last checkpoint   
            changessql = "DECLARE  @from_lsn binary(10), @to_lsn binary(10); " 
            # comment this if statement if you wish to retreive all rows every time
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
            select [__$operation],[id],[Name],coalesce(resources,paths) Resources,[Status],replace(permMapList,'''','"') permMapList,[Service Type] 
            from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all update old') 
            order by id,__$seqval,__$operation;"""

            #print(changessql)
            # determine if last checkpoint is available and if so read all the changes into a pandas dataframe
            changesdf= pandas.io.sql.read_sql(changessql, cnxn)
            print(changesdf)
            if changesdf.empty:

              print("No changes found. Exiting...")
              ## TODO This needs to be reworked because we should still update the checkpoint if no rows were found
              
              now =  datetime.datetime.utcnow()
              progendtime = now.strftime('%Y-%m-%d %H:%M:%S')

              # save checkpoint in control table
              set_ct_info = "insert into " + dbname + "." + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + appname + "', current_timestamp,'" + progstarttime + "','"+ progendtime + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
              #print(set_ct_info)
              cursor.execute(set_ct_info)
              # now terminate the program
              return
            else:
               uniquepolicyids = changesdf.groupby('id').id.nunique()
               policy_rows_changed  =[uniquepolicyids.value_counts()][0][1] # get the number of unique policy ids in the changed record set which will be stored on the control table at the end of the routine
               print("Number of unique policy records changed: " + str(policy_rows_changed))
               

            # CDC operation 1 = deleted record
            deleteddf = changesdf[(changesdf['__$operation']==1)]
            # CDC operation 2 = inserted record, filter by new policies entries
            insertdf = changesdf[(changesdf['__$operation']==2)]
            # CDC operation 3 is the before image of the row, operation 4 is the after (current) image.
            updatesdf = changesdf[(changesdf['__$operation']==3) |(changesdf['__$operation']==4)]

            if not (insertdf.empty and updatesdf.empty and deleteddf.empty): # if there are either inserts or updates then only get tokens
                storagetoken = getBearerToken("storage.azure.com",spnid,spnsecret)
                graphtoken = getBearerToken("graph.microsoft.com",spnid,spnsecret)

            #################################################
            #                                               #
            #                 New Policies                  #
            #                                               #
            ################################################# 

            if not insertdf.empty:
                #there are changes to process. first obtain an AAD tokens

                print("\nNew policy rows to apply:")
                print(insertdf)
                print("\n")

                # iterate through the new policy rows
                for row in insertdf.loc[:, ['Resources','Status','permMapList','Service Type']].itertuples():
                    
                    if row.Status in ('Enabled','True') : 

                        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                        hdfsentries = row.Resources.strip("path=[").strip("[").strip("]").split(",")

                        #Load the json string of permission mappings into a dictionary object
                        permmaplist = json.loads(row.permMapList)

                        for permap in permmaplist: #this loop iterates through each permMapList and applies the ACLs
                            for perm in permap["permList"]:
                                print("Permission: "+perm)
                            # determine the permissions rwx
                            permstr = getPermSeq(permap["permList"])    
                            permstr = permstr.ljust(3,'-')
                            print("Permissions to be set: " +permstr)
                            for groups in permap["groupList"]:
                                print("Groups: " + groups)
                            for userList in permap["userList"]:
                                print("Users: " + userList)

                            # obtain a list of all security principals
                            spids = getSPIDs(permap["userList"],permap["groupList"])
                            if spids: # only set perms if there was at least one user found
                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    print('calling bulk set')
                                    acl_change_counter += setADLSBulkPermissions(storagetoken, spids, hdfsentry, permstr)
                            else:
                                print("!!!!!!!!Error: No permissions could be set as none of the users found in AAD!!!!!!!!!!!!!!")        

            elif not deleteddf.empty:
            #################################################
            #                                               #
            #               Deleted Policies                #
            #                                               #
            ################################################# 


                print("\nDeleted policy rows to apply:")
                print(deleteddf)
                print("\n")

                # iterate through the deleted policy rows
                for row in deleteddf.loc[:, ['Resources','Groups','Users','Accesses','Status','permMapList','Service Type']].itertuples():
                    
                    if row.Status in ('Enabled','True'): # only bother deleting ACLs where the policy was in an enabled state

                        #Load the json string of permission mappings into a dictionary object
                        permmaplist = json.loads(row.permMapList)
                        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                        hdfsentries = row.Resources.strip("path=[").strip("]").split(",")

                        for permap in permmaplist: #this loop iterates through each permMapList and applies the ACLs
                            # no need to determine the permissions in a delete/remove ACL scernario
                            ##permstr = getPermSeq(row.Accesses.split(","))    
                            
                            # obtain a list of all security principals
                            spids = getSPIDs(permap["userList"],permap["groupList"])

                            for hdfsentry in hdfsentries:
                                hdfsentry = hdfsentry.strip().strip("'")
                                print('calling bulk remove...')
                                acl_change_counter += removeADLSBulkPermissions(storagetoken, spids, hdfsentry)


            else:
                print("No new or deleted policies detected. ")

            print("Determining any other changes...")

            #################################################
            #                                               #
            #             Modified Policies                 #
            #                                               #
            ################################################# 

            rowid = -1 # set this to some policy ID that can never exist so the if statement below finds a new policy ID to process
            for index, row in updatesdf.iterrows():
              # loop through all the rows but only execute the modified policy logic when we find a new ID to process
              if rowid != row['id']:

                # new policy ID detected, now set the previous value with the new one
                rowid = row['id']

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

                # Comment: 11b
                # Fetch the first and last row of changes for a particular ID. This is because we are only concerned with the before 
                # and after snapshot, even if multiple changes took place. e.g. if a complex scenario arises in one run such as removal of users/groups, addition of new users/groups, as well as changes to permissions or paths,
                # this is handled as a recursive deletion of these users/groups from the ACLs from the previous image of the path (in case this was part of the changed fields), and then the new users/groups are recursively added to the latest image of the path
                firstandlastforid = updatesdf[(updatesdf['id']==rowid)].iloc[[0, -1]]
                #print("debug: "+ str(firstandlastforid.get(key = 'Resources')[0]))
                #print("debug: "+ str(firstandlastforid.get(key = 'Resources')[3]))                
                #print("index: "+str(firstandlastforid.tail(1).index[0]))
                #print("debug: "+ str(firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]]))

                if row['Service Type']=='hive':
                    resourcesbefore = firstandlastforid.get(key = 'paths')[0].split(",")
                    resourcesafter = firstandlastforid.get(key = 'paths')[firstandlastforid.tail(1).index[0]].split(",")
                elif row['Service Type']=='hdfs':
                    resourcesbefore = firstandlastforid.get(key = 'Resources')[0].strip("path=[").strip("]").split(",")
                    resourcesafter = firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]].strip("path=[").strip("]").split(",")   # note the syntax [firstandlastforid.tail(1).index[0]] fetches the index of the last record in case there were multiple changes
                else:
                    resourcesbefore = ''
                    resourcesafter = ''

                statusbefore = firstandlastforid.get(key = 'Status')[0].strip()
                statusafter = firstandlastforid.get(key = 'Status')[firstandlastforid.tail(1).index[0]].strip()

                # load the permMapList into a json aray
                permMapBefore = json.loads(firstandlastforid.get(key = 'permMapList')[0])
                permMapAfter = json.loads(firstandlastforid.get(key = 'permMapList')[firstandlastforid.tail(1).index[0]])

                # obtain the length of each array and then get the max - this will be the number of iterations that we are comparing for changes. any index that is out of bounds is essentially a delete operation because it has been removed
                maplistcountbefore = len(permMapBefore)
                maplistcountafter = len(permMapAfter)

                maplistmax = max(maplistcountbefore,maplistcountafter)
                print("maplistcount before: "+str(maplistcountbefore) + " after:" + str(maplistcountafter) + " max "+ str(maplistmax))
                print("permmapbf: " + str(permMapBefore))
                print("permmapaf: " + str(permMapAfter))
                # iterate through the permaplist array
                for n in range(0,maplistmax):
                    accessesbefore = ''
                    groupsbefore = ''
                    usersbefore = ''
                    
                    # determine  access before and after image
                    # trap the index out of error exception as this means either the array index didn't exist in either the before or after i.e. a permmaplist was added or deleted
                    try:

                        # determine perms, group and user before images
                        accessesbefore = permMapBefore[n]["permList"]
                        if permMapBefore[n]["groupList"]: 
                            groupsbefore = permMapBefore[n]["groupList"]
                            groupsbefore = [groupitem.strip(' ') for groupitem in groupsbefore] # clean up by removing any spaces
                        if permMapBefore[n]["userList"]: 
                            usersbefore =permMapBefore[n]["userList"]
                            usersbefore = [useritem.strip(' ') for useritem in usersbefore] # clean up by removing any spaces

                    except IndexError:
                      accessesbefore = ''
                      groupsbefore = ''
                      usersbefore = ''
                    try:
                        # determine perms, group and user after images
                        accessesafter = permMapAfter[n]["permList"]
                        permstr = getPermSeq(accessesafter)    
                        if permMapAfter[n]["groupList"]: 
                            groupsafter = permMapAfter[n]["groupList"]
                            groupsafter = [groupitem.strip(' ') for groupitem in groupsafter] # clean up by removing any spaces
                        if permMapAfter[n]["userList"]: 
                            usersafter = permMapAfter[n]["userList"]
                            usersafter = [useritem.strip(' ') for useritem in usersafter] # clean up by removing any spaces

                    except IndexError:
                      accessesafter = ''
                      usersafter = ''
                      groupsafter = ''
                      permstr = ''
                   
                    # now determine the differences between users and groups before and after
                    print(str(n) + " - groups before = " + str(groupsbefore) + ". Groups after " + str(groupsafter)) 
                    print(str(n) + " - groups before = " + str(usersbefore) + ". Groups after " + str(usersafter)) 
                    addgroups = entitiesToAdd(groupsbefore,groupsafter)
                    removegroups = entitiesToRemove(groupsbefore,groupsafter)    
                    addusers = entitiesToAdd(usersbefore,usersafter)
                    removeusers = entitiesToRemove(usersbefore,usersafter)    

                    #check if they are really different even if the order was simply changed
                    if addgroups or removegroups:
                        if check_if_equal(addgroups, removegroups):
                            print('Groups in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                            addgroups = None
                            removegroups = None

                    if addusers or removeusers:
                        if check_if_equal(addusers, removeusers):
                            print('Users in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                            addusers = None
                            removeusers = None

                    # determine if any permissions changed
                    # note we do not actually need to calculate the differences in permissions because they must be done as a delete of ACLs using the before image
                    # and an addition of new ACLs using the after image ie there is no incremental way to remove just a read or a write permission individually AFAIK
                    addaccesses = entitiesToAdd(accessesbefore,accessesafter)
                    removeaccesses = entitiesToRemove(accessesbefore,accessesafter) 

                    # determine whether policy status changed
                    if statusbefore != statusafter:
                        if statusafter in ('Enabled','True'): # an enabled policy is treated as a new policy
                            print('Policy now enabled, same as a new policy - add ACLS')    

                            
                            # obtain a dictionary list of all security principals
                            spids = getSPIDs(usersafter,groupsafter)

                            # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                            for resourceentry in resourcesafter:
                                resourceentry = resourceentry.strip().strip("'")
                                print('calling bulk set')
                                acl_change_counter += setADLSBulkPermissions(storagetoken, spids, resourceentry.strip(), permstr)

                        if statusafter not in ('Enabled','True'): # a disabled policy is treated in the same way as a deleted policy
                            print('Policy now disabled therefore delete ACLs')  

                            spids = getSPIDs(usersafter,groupsafter)

                            for resourceentry in resourcesafter:
                                resourceentry = resourceentry.strip().strip("'")
                                print('calling bulk remove per directory path')
                                acl_change_counter += removeADLSBulkPermissions(storagetoken, spids, resourceentry)

                    elif row.Status in ('Enabled','True'): # if there wasn't a status change, then only bother dealing with modifications is the policy is set to enabled i.e. we don't care about policies that were disabled prior to the change and are still not enabled. when / if they are eventually enabled they will be treated as any new policy would
                        
                        # determine resources (path) changes. A path change will be apply as a delete of the previous values (users/groups/perms) and an addition of the new values, hence no need to process any further changes as this will bring the entire policy record up to date   
                        addresources = entitiesToAdd(resourcesbefore,resourcesafter)
                        removeresources = entitiesToRemove(resourcesbefore,resourcesafter)   
                        if removeresources or addresources: 
                            if removeresources:
                                print("remove all previous permissions from the following resources: ")
                                spids = getSPIDs(usersbefore,groupsbefore)
                                for resourcetoremove in removeresources:
                                    resourcetoremove = resourcetoremove.strip().strip("'")
                                    print(resourcetoremove)
                                    acl_change_counter += removeADLSBulkPermissions(storagetoken, spids, resourcetoremove)

                            if addresources:
                                print("add the new permissions to the following resources")
                                spids = getSPIDs(usersafter,groupsafter)
                                for resourcetoadd in addresources:
                                    resourcetoadd = resourcetoadd.strip().strip("'")
                                    print(resourcetoadd)
                                    acl_change_counter += setADLSBulkPermissions(storagetoken, spids, resourcetoadd, permstr)

                        # only process incremental changes to groups or users, if there wasn't either a status change or a path change
                        elif addgroups or addusers or removegroups or removeusers: 
                            #########################
                            # determine user or group changes
                            ##########################


                            if removegroups or removeusers:
                                if removegroups:
                                    print("Remove the following groups: ")
                                    for grouptoremove in removegroups:
                                        print(grouptoremove)
                                if removeusers:
                                    print("Remove the following users: ")
                                    for usertoremove in removeusers:
                                        print(usertoremove)

                                # get associated IDs for the user/groups to be removed
                                spids = getSPIDs(removeusers,removegroups)

                                for resourceentry in resourcesbefore:
                                    resourceentry = resourceentry.strip().strip("'")
                                    print('calling bulk remove per directory path')
                                    acl_change_counter += removeADLSBulkPermissions(storagetoken, spids,  resourceentry)

                            if addgroups or addusers: 
                                if addgroups:
                                    print("Add the following groups: ")
                                    for grouptoadd in addgroups:
                                        print(grouptoadd)
                                if addusers:    
                                    print("Add the following users")
                                    for usertoadd in addusers:
                                        print(usertoadd)

                                spids = getSPIDs(addusers,addgroups)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference
                         
                                for resourceentry in resourcesafter:
                                    resourceentry = resourceentry.strip().strip("'")
                                    print('calling bulk set')
                                    acl_change_counter += setADLSBulkPermissions(storagetoken, spids, resourceentry, permstr)

                        elif  removeaccesses or addaccesses:  # only process changes to permissions if they were done as part of another change above!
                            #######################################
                            # determine access/permissions changes
                            ######################################
                            # note we do not actually need to calculate the differences in permissions because they must be done as a delete of ACLs using the before image and an addition of new ACLs using the after image ie there is no incremental way to remove just a read or a write permission individually AFAIK
                            if removeaccesses:
                                print("remove the following accesses")
                                for accesstoremove in removeaccesses:
                                    print(accesstoremove)

                                spids = getSPIDs(groupsbefore,usersbefore)
                                for resourceentry in resourcesbefore:
                                    resourceentry = resourceentry.strip().strip("'")
                                    print('calling bulk remove per directory path')
                                    acl_change_counter += removeADLSBulkPermissions(storagetoken, spids,  resourceentry)

                            if addaccesses:
                                print("add the following accesses")
                                for accesstoadd in addaccesses:
                                    print(accesstoadd)

                                spids = getSPIDs(usersafter,groupsafter)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference

                                for resourceentry in resourcesafter:
                                    resourceentry = resourceentry.strip().strip("'")
                                    print('calling bulk set')
                                    acl_change_counter += setADLSBulkPermissions(storagetoken, spids, resourceentry, permstr)

                        else:
                            print("Changes were identified but no action taken. This could be due to: \n- A policy entry was updated but the fields stayed the same, just the order of the entities changed. \n- A change occured that did not pertain to paths, users, groups, permissions. \nThese change have been ignored.")
                    else: 
                        print("No other changes to process.")


            now =  datetime.datetime.utcnow()
            progendtime = now.strftime('%Y-%m-%d %H:%M:%S')

            # save checkpoint in control table
            set_ct_info = "insert into " + dbname + "." + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + appname + "', current_timestamp,'" + progstarttime + "','"+ progendtime + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
            #print(set_ct_info)
            #cursor.execute(set_ct_info)

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            print('Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            print('Done')
    finally:
            cnxn.autocommit = True


## DEPRECATED - see newer method with bulk override
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
    return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))

# a variation of the function above which access a dictionary object of users and groups so that we can set the ACLs in bulk with a comma seprated list of ACEs (access control entries)
def setADLSBulkPermissions(aadtoken, spids, adlpath, permissions):
    if spids:
        acentry = ""
        for sp in spids:
            #print('SPID' + str(spids[sp]))
            for spid in spids[sp]:
                #print("Preparing " + sp + ' permissions for ' + spid)
                acentry += sp+':'+spid+ ':'+permissions+',default:'+sp+':'+spid + ':'+permissions +','
        acentry = acentry.rstrip(',') 
        #print("Setting permission: "+acentry)
        #print(acentry.rstrip(','))
        basestorageuri = 'https://baselake.dfs.core.windows.net/base'
        spnaccsuffix = ''
        # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
        puuid = str(uuid.uuid4())
        headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl':acentry,'x-ms-client-request-id': '%s' % puuid}
        request_path = basestorageuri+adlpath+"?action=setAccessControlRecursive&mode=modify"
        print("Setting " + permissions + " ACLs  " + acentry + " on " +adlpath + "...")
        t1_start = perf_counter() 
        if devstage == 'live':
            r = requests.patch(request_path, headers=headers)
            response = r.json()
            t1_stop = perf_counter()
            #print(r.text)
            if r.status_code == 200:
                print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
            else:
                print("Error: " + str(r.text))
            return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
        else:
            return(0)
    else:
        print("Warning: Could not set ACLs as no users/groups were found in AAD")    
        return(0)
    #aces = spntype+':'+spn+spnaccsuffix + ':'+permissions+',default:'+spntype+':'+spn+spnaccsuffix + ':'+permissions,'x-ms-client-request-id': '%s' % puuid


def removeADLSBulkPermissions(aadtoken, spids, adlpath):
    ## no permissions str required in a remove call
    acentry = ""
    if spids:
        for sp in spids:
            #print(spids[sp])
            for spid in spids[sp]:
            #print("Preparing " + sp + ' permissions for ' + spid)
              acentry += sp+':'+spid +',default:'+sp+':'+spid +','
        acentry = acentry.rstrip(',') 
        basestorageuri = 'https://baselake.dfs.core.windows.net/base'
        spnaccsuffix = ''
        #print(spn + '-' + adlpath)
        # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
        #Setup the endpoint
        puuid = str(uuid.uuid4())
        #print('Log analytics UUID'+ puuid)
        headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': acentry,'x-ms-client-request-id': '%s' % puuid}
        request_path = basestorageuri+adlpath+"?action=setAccessControlRecursive&mode=remove"
        print("Removing ACLs: " + acentry + " on " +adlpath + "...")
        t1_start = perf_counter() 
        if devstage == 'live':
            r = requests.patch(request_path, headers=headers)
            response = r.json()
            t1_stop = perf_counter()

            if r.status_code == 200:
                print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
            else:
                print("Error: " + str(r.text))
            return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
        else:
            return(0)
        #print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
    else:
        print("Warning: Could not set ACLs as no users/groups were found in AAD")    
        return(0)

## DEPRECATED - see newer method with bulk override
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
    return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))

  

def getBearerToken(resourcetype,spnid,spnsecret):
    endpoint = 'https://login.microsoftonline.com/af26513a-fe59-4005-967d-bd744f659830/oauth2/token'

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'grant_type=client_credentials&client_id='+spnid+'&client_secret='+ spnsecret + '&resource=https%3A%2F%2F'+resourcetype+'%2F'
    r = requests.post(endpoint, headers=headers, data=payload)
    response = r.json()
    print("Obtaining AAD bearer token for resource "+ resourcetype + "...")
    #print(response)
    bearertoken = response["access_token"]
    #print(bearertoken)
    print("Bearer token obtained.\n")
    return bearertoken

devstage = 'debug'
getPolicyChanges()


