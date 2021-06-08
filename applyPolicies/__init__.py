from asyncio.windows_events import NULL
import datetime
import logging
import os
import uuid
import asyncio
from typing import Any, Callable, ClassVar
import urllib
import pyodbc
import pandas as pd
import pandas.io.common
from sqlalchemy import create_engine
from sqlalchemy import event
import sqlalchemy
import azure.functions as func
import requests
import uuid
from requests.auth import HTTPBasicAuth
from time import perf_counter
from collections import defaultdict

from collections import namedtuple
import azure.durable_functions as df

from applyPolicies import AclUpdaterService as aclService
from applyPolicies import GraphApiService as graph

global STORAGE_URL
global STORAGE_NAME

STORAGE_URL = "https://centricapoc.dfs.core.windows.net/"
STORAGE_NAME = "centricapoc"

global cnxn


async def getPolicyChanges(starter:str):

    def getPermSeq(perms):
        permstr = ''
        for perm in perms:
            if perm.strip() == 'read':
                permstr = 'r'
            elif perm.strip() == 'write':
                permstr += 'w'
            elif perm.strip() == 'execute':
                permstr += 'x'
            else:
                permstr += '-'
        return permstr

    def getSPID(aadtoken, spname, spntype):

        # temp
        if (spname == 'dawid.obrocki@microsoft.com'):
            spname = 'dawid.obrocki_microsoft.com#EXT#@contactliammoat.onmicrosoft.com'
        
        search_result = graph.find(filter_type=spntype, search_phase=spname.strip().replace('#', '%23'))

        if  search_result == '':
            print("Warning: Could not find user ID!!!")
        
        return search_result

    def getSPIDs(userslist, groupslist):
        # a dictionary object of all the security principal (sp) IDs to be set in this ACL
        spids = defaultdict(list)

        # iterate through the comma separate list of groups and set the dictionary object
        if userslist is not None and len(userslist) > 0:
            userentries = str(userslist).split(",")
            for userentry in userentries:
                #print("user: "+userentry.strip("['").strip("']").strip(' '))
                spnid = getSPID(graphtoken, userentry.strip(
                    "['").strip("']").strip("'").strip(' '), 'users')
                spids['user'].append(spnid)

        # iterate through the comma separate list of groups and set the dictionary object
        if groupslist is not None and len(groupslist) > 0:
            groupentries = str(groupslist).split(",")
            for groupentry in groupentries:
                spnid = getSPID(graphtoken, groupentry.strip(
                    "['").strip("']").strip("'").strip(' '), 'groups')
                if spnid is not None:
                    spids['group'].append(spnid)
        return spids

    try:

        aclService.initialize(storageName=STORAGE_NAME,
                              accountKey=os.environ["StorageAccoutKey"])
                
        dbschema = "dbo"
        dbname = 'policystore'
        connxstr = os.environ["DatabaseConnxStr"]
        print("Connection string: " + connxstr)

        # configure database params

        spnid = os.environ["SPNID"]
        spnsecret = os.environ["SPNSecret"]
        appname = 'applyPolicies'

        stagingtablenm = "ranger_policies_staging"
        targettablenm = "ranger_policies"
        batchsize = 200
        params = urllib.parse.quote_plus(connxstr+'Database='+dbname + ';')
        collist = ['ID', 'Name', 'Resources', 'Groups',
                   'Users', 'Accesses', 'Service Type', 'Status']
        # ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

        cnxn = pyodbc.connect(connxstr)
        cursor = cnxn.cursor()
        now = datetime.datetime.utcnow()
        progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
        get_ct_info = "select lsn_checkpoint from " + dbname + "." + dbschema + \
            ".policy_ctl where id= (select max(id) from " + dbname + "." + \
            dbschema + ".policy_ctl where application = '" + appname + "');"
        # print(get_ct_info)
        print("Getting control table information...")
        cursor.execute(get_ct_info)
        row = cursor.fetchone()
        lsn_checkpoint = None
        acl_change_counter = 0
        policy_rows_changed = 0

        if row:
            print("Last checkpoint was at "+str(row[0]))
            lsn_checkpoint = row[0]
        else:
            print("No control information, obtaining all changes...")

        # changessql is the string variable which holds the SQL statements to determine the changes since the last checkpoint
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
            # cursor.cancel()
        changessql = changessql + """            
            select [__$operation],[id],[Name],[Resources],[Groups],[Users],[Accesses],[Status] 
            from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm + """(@from_lsn, @to_lsn, 'all update old') 
            order by id,__$seqval,__$operation;"""

        # print(changessql)
        # determine if last checkpoint is available and if so read all the changes into a pandas dataframe
        changesdf = pandas.io.sql.read_sql(changessql, cnxn)
        # print(changesdf)
        if changesdf.empty:

            print("No changes found. Exiting...")
            # TODO This needs to be reworked because we should still update the checkpoint if no rows were found

            now = datetime.datetime.utcnow()
            progendtime = now.strftime('%Y-%m-%d %H:%M:%S')

            # save checkpoint in control table
            set_ct_info = "insert into " + dbname + "." + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + \
                appname + "', current_timestamp,'" + progstarttime + "','" + progendtime + \
                "'," + str(policy_rows_changed) + "," + \
                str(acl_change_counter)+")"
            # print(set_ct_info)
            cursor.execute(set_ct_info)
            # now terminate the program
            exit()
        else:
            uniquepolicyids = changesdf.groupby('id').id.nunique()
            # get the number of unique policy ids in the changed record set which will be stored on the control table at the end of the routine
            policy_rows_changed = [uniquepolicyids.value_counts()][0][1]
            print("Number of unique policy records changed: " +
                  str(policy_rows_changed))

        # CDC operation 1 = deleted record
        deleteddf = changesdf[(changesdf['__$operation'] == 1)]
        # CDC operation 2 = inserted record, filter by new policies entries
        insertdf = changesdf[(changesdf['__$operation'] == 2)]
        # CDC operation 3 is the before image of the row, operation 4 is the after (current) image.
        updatesdf = changesdf[(changesdf['__$operation'] == 3) | (
            changesdf['__$operation'] == 4)]

        # if there are either inserts or updates then only get tokens
        if not (insertdf.empty and updatesdf.empty and deleteddf.empty):
            graphtoken = getBearerToken(
                "graph.microsoft.com", spnid, spnsecret)
            storagetoken = getBearerToken(
                "storage.azure.com", spnid, spnsecret)

        #################################################
        #                                               #
        #                 New Policies                  #
        #                                               #
        #################################################

        if not insertdf.empty:
            # there are changes to process. first obtain an AAD tokens

            print("\nNew policy rows to apply:")
            print(insertdf)
            print("\n")

            # iterate through the new policy rows
            for row in insertdf.loc[:, ['Resources', 'Groups', 'Users', 'Accesses', 'Status']].itertuples():
                
                if row.Status == 'Enabled':
                    # determine the permissions rwx
                    permstr = getPermSeq(row.Accesses.split(","))

                    # obtain a dictionary list of all security principals
                    spids = getSPIDs(row.Users, row.Groups)

                    # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                    hdfsentries = row.Resources.strip(
                        "path=[").strip("]").split(",")


                    new_policy_tasks = []
                    
                    #PermissionUpdateParams = namedtuple('PermissionUpdateParams', ['SPIDs', 'AdlPath', 'Permissions'])              

                    # for hdfsentry in hdfsentries:
                    #     print('calling bulk set')
                    # Run as Tasks
                    #     new_policy_tasks.append(asyncio.create_task(setADLSBulkPermissions(storagetoken, spids, hdfsentry.strip(), permstr)))
                                            
                        
                    payloadToOrchestrate = []

                    try:
                        for hdfsentry in hdfsentries:
                            payload = {"SPIDs": spids, "AdlPath": hdfsentry.strip(), "Permissions":permstr }    
                            payloadToOrchestrate.append(payload)                            
                                                                    
                        
                        client = df.DurableOrchestrationClient(starter)
                        startTimer = perf_counter()                
                        await client.start_new("applyPoliciesOrchestrator", None, client_input=payloadToOrchestrate)
                        stopTimer = perf_counter()
                    except Exception as e:
                        print(e)
                                        
                    #instance_id = await client.start_new() (orchestrator_function(starter))                        
                                              
                    
                    

                    print(f'New Policies tasks completed in {stopTimer-startTimer:.3f}')



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
            for row in deleteddf.loc[:, ['Resources', 'Groups', 'Users', 'Accesses', 'Status']].itertuples():

                if row.Status == 'Enabled':  # only bother deleting ACLs where the policy was in an enabled state
                    # no need to determine the permissions in a delete/remove ACL scernario
                    ##permstr = getPermSeq(row.Accesses.split(","))

                    # obtain a dictionary list of all security principals
                    spids = getSPIDs(row.Users, row.Groups)

                    # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                    hdfsentries = row.Resources.strip(
                        "path=[").strip("]").split(",")


                    delete_policy_tasks = []

                    for hdfsentry in hdfsentries:
                        print('calling bulk remove...')
                        delete_policy_tasks.append(asyncio.create_task(removeADLSBulkPermissions(storagetoken, spids, hdfsentry.strip())))
                    
                    startTimer = perf_counter()                    
                    await asyncio.wait(delete_policy_tasks)                    
                    stopTimer = perf_counter()

                    print(f'Delete policies tasks completed in {stopTimer-startTimer:.3f}')

        else:
            print("No new or deleted policies detected. ")

        print("Determining any other changes...")

        #################################################
        #                                               #
        #             Modified Policies                 #
        #                                               #
        #################################################

        rowid = -1  # set this to some policy ID that can never exist so the if statement below finds a new policy ID to process
        for index, row in updatesdf.iterrows():
            # loop through all the rows but only execute the modified policy logic when we find a new ID to process
            if rowid != row['id']:

                # new policy ID detected, now set the previous value with the new one
                rowid = row['id']

                # reset the arrays per unique policy ID
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
                firstandlastforid = updatesdf[(
                    updatesdf['id'] == rowid)].iloc[[0, -1]]

                # note to reader:
                # don't be confused by the for loops below -
                # it must seem a really odd way to get a single row from the pandas series but I couldn't determine another way at time of development.
                # essentially, this is just fetching the one row from each iterrows to store the before and after value from the iloc 0,-1 filter above
                for index, rowbefore in firstandlastforid.iloc[[0]].iterrows():
                    print("Found changes for policy id "+str(rowbefore['id']))

                    # determine resources (path), access and status before image
                    resourcesbefore = rowbefore.Resources.strip(
                        "path=[").strip("]").split(",")
                    accessesbefore = rowbefore.Accesses.split(",")
                    statusbefore = rowbefore.Status

                    # determine group and user before images
                    if rowbefore.Groups:
                        groupsbefore = rowbefore.Groups.split(",")
                        # clean up by removing any spaces
                        groupsbefore = [groupitem.strip(
                            ' ') for groupitem in groupsbefore]
                    if rowbefore.Users:
                        usersbefore = rowbefore.Users.split(",")
                        # clean up by removing any spaces
                        usersbefore = [useritem.strip(
                            ' ') for useritem in usersbefore]

                for index, rowafter in firstandlastforid.iloc[[1]].iterrows():

                    # determine resources (path), access and status after image
                    resourcesafter = rowafter.Resources.strip(
                        "path=[").strip("]").split(",")
                    accessesafter = rowafter.Accesses.split(",")
                    statusafter = rowafter.Status.strip()

                    # determine group and user after images
                    if rowafter.Groups:
                        groupsafter = rowafter.Groups.split(",")
                        # clean up by removing any spaces
                        groupsafter = [groupitem.strip(
                            ' ') for groupitem in groupsafter]
                    if rowafter.Users:
                        usersafter = rowafter.Users.split(",")
                        # clean up by removing any spaces
                        usersafter = [useritem.strip(' ')
                                      for useritem in usersafter]

                # utility functions to determine differences in lists

                def entitiesToAdd(beforelist, afterlist):
                    return (list(set(afterlist) - set(beforelist)))

                def entitiesToRemove(beforelist, afterlist):
                    return (list(set(beforelist) - set(afterlist)))

                # determine whether the lists are equal if sorted i.e. the same elements just in different order
                def check_if_equal(list_1, list_2):
                    """ Check if both the lists are of same length and if yes then compare
                    sorted versions of both the list to check if both of them are equal
                    i.e. contain similar elements with same frequency. """
                    if len(list_1) != len(list_2):
                        return False
                    return sorted(list_1) == sorted(list_2)

                # now determine the differences between users and groups before and after
                #print("Groups before = " + str(groupsbefore) + " Groups after " + str(groupsafter))
                addgroups = entitiesToAdd(groupsbefore, groupsafter)
                removegroups = entitiesToRemove(groupsbefore, groupsafter)
                addusers = entitiesToAdd(usersbefore, usersafter)
                removeusers = entitiesToRemove(usersbefore, usersafter)

                # check if they are really different even if the order was simply changed
                if addgroups or removegroups:
                    if check_if_equal(addgroups, removegroups):
                        print(
                            'Groups in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                        addgroups = None
                        removegroups = None

                if addusers or removeusers:
                    if check_if_equal(addusers, removeusers):
                        print(
                            'Users in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                        addusers = None
                        removeusers = None

                # determine if any permissions changed
                # note we do not actually need to calculate the differences in permissions because they must be done as a delete of ACLs using the before image
                # and an addition of new ACLs using the after image ie there is no incremental way to remove just a read or a write permission individually AFAIK
                addaccesses = entitiesToAdd(accessesbefore, accessesafter)
                removeaccesses = entitiesToRemove(
                    accessesbefore, accessesafter)

                permstr = getPermSeq(rowafter.Accesses.split(","))

                # determine whether policy status changed
                if statusbefore != statusafter:
                    if statusafter == 'Enabled':  # an enabled policy is treated as a new policy
                        print('Policy now enabled, same as a new policy - add ACLS')

                        # obtain a dictionary list of all security principals
                        spids = getSPIDs(rowafter.Users, rowafter.Groups)

                        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                        hdfsentries = rowafter.Resources.strip(
                            "path=[").strip("]").split(",")

                        mod_policy_tasks = []
                        for hdfsentry in hdfsentries:
                            print('calling bulk set')
                            mod_policy_tasks.append(asyncio.create_task(setADLSBulkPermissions(storagetoken, spids, hdfsentry.strip(), permstr)))

                        startTimer = perf_counter()                    
                        await asyncio.wait(new_policy_tasks)                    
                        stopTimer = perf_counter()
                        print(f'Mod Policies tasks completed in {stopTimer-startTimer:.3f}')


                    if statusafter == 'Disabled':  # a disabled policy is treated in the same way as a deleted policy
                        print('Policy now disabled therefore delete ACLs')

                        spids = getSPIDs(rowafter.Users, rowafter.Groups)
                        hdfsentries = rowafter.Resources.strip(
                            "path=[").strip("]").split(",")
                        for hdfsentry in hdfsentries:
                            print('calling bulk remove per directory path')
                            acl_change_counter += removeADLSBulkPermissions(
                                storagetoken, spids, hdfsentry.strip())

                elif row.Status == 'Enabled':  # if there wasn't a status change, then only bother dealing with modifications is the policy is set to enabled i.e. we don't care about policies that were disabled prior to the change and are still not enabled. when / if they are eventually enabled they will be treated as any new policy would

                    # determine resources (path) changes. A path change will be apply as a delete of the previous values (users/groups/perms) and an addition of the new values, hence no need to process any further changes as this will bring the entire policy record up to date
                    addresources = entitiesToAdd(
                        resourcesbefore, resourcesafter)
                    removeresources = entitiesToRemove(
                        resourcesbefore, resourcesafter)
                    if removeresources or addresources:
                        if removeresources:
                            print(
                                "remove all previous permissions from the following resources: ")
                            for resourcetoremove in removeresources:
                                print(resourcetoremove)
                                spids = getSPIDs(
                                    rowbefore.Users, rowbefore.Groups)
                                hdfsentries = rowbefore.Resources.strip(
                                    "path=[").strip("]").split(",")
                                for hdfsentry in hdfsentries:
                                    print(
                                        'calling bulk remove per directory path')
                                    acl_change_counter += removeADLSBulkPermissions(
                                        storagetoken, spids, resourcetoremove)

                        if addresources:
                            print(
                                "add the new permissions to the following resources")
                            for resourcetoadd in addresources:
                                print(resourcetoadd)
                                spids = getSPIDs(
                                    rowafter.Users, rowafter.Groups)
                                # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                                
                                new_policy_tasks = []
                                        
                                for hdfsentry in hdfsentries:
                                    print('calling bulk set')
                                    new_policy_tasks.append(asyncio.create_task(setADLSBulkPermissions(storagetoken, spids, hdfsentry.strip(), permstr)))

                                startTimer = perf_counter()                    
                                await asyncio.wait(new_policy_tasks)                    
                                stopTimer = perf_counter()
                                print(f'New Policies tasks completed in {stopTimer-startTimer:.3f}')

                    # only process incremental changes to groups or users, if there wasn't either a status change or a path change
                    elif removegroups or removeusers:
                        #########################
                        # determine user or group changes
                        ##########################

                        if removegroups or removeusers:
                            if removegroups:
                                print("Remove the following groups = " +
                                      str(removegroups))
                                for grouptoremove in removegroups:
                                    print(grouptoremove)
                            if removeusers:
                                print("Remove the following users")
                                for usertoremove in removeusers:
                                    print(usertoremove)

                            # get associated IDs for the user/groups to be removed
                            spids = getSPIDs(removeusers, removegroups)
                            hdfsentries = rowbefore.Resources.strip(
                                "path=[").strip("]").split(",")
                            for hdfsentry in hdfsentries:
                                print('calling bulk remove per directory path')
                                acl_change_counter += removeADLSBulkPermissions(
                                    storagetoken, spids,  hdfsentry.strip())

                        if addgroups or addusers:
                            if addgroups:
                                print("add the following groups = " +
                                      str(addgroups))
                                for grouptoadd in addgroups:
                                    print(grouptoadd)
                            if addusers:
                                print("add the following users")
                                for usertoadd in addusers:
                                    print(usertoadd)

                            # Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference
                            spids = getSPIDs(addusers, addgroups)
                            # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                            hdfsentries = rowafter.Resources.strip(
                                "path=[").strip("]").split(",")
                            new_policy_tasks = []
                                        
                            for hdfsentry in hdfsentries:
                                print('calling bulk set')
                                new_policy_tasks.append(asyncio.create_task(setADLSBulkPermissions(storagetoken, spids, hdfsentry.strip(), permstr)))

                            startTimer = perf_counter()                    
                            await asyncio.wait(new_policy_tasks)                    
                            stopTimer = perf_counter()
                            print(f'New Policies tasks completed in {stopTimer-startTimer:.3f}')

                    elif removeaccesses or addaccesses:  # only process changes to permissions if they were done as part of another change above!
                        #######################################
                        # determine access/permissions changes
                        ######################################
                        # note we do not actually need to calculate the differences in permissions because they must be done as a delete of ACLs using the before image and an addition of new ACLs using the after image ie there is no incremental way to remove just a read or a write permission individually AFAIK
                        if removeaccesses:
                            print("remove the following accesses")
                            for accesstoremove in removeaccesses:
                                print(accesstoremove)

                            spids = getSPIDs(rowbefore.Groups, rowbefore.Users)
                            hdfsentries = rowbefore.Resources.strip(
                                "path=[").strip("]").split(",")
                            for hdfsentry in hdfsentries:
                                print('calling bulk remove per directory path')
                                acl_change_counter += removeADLSBulkPermissions(
                                    storagetoken, spids,  hdfsentry.strip())

                        if addaccesses:
                            print("add the following accesses")
                            for accesstoadd in addaccesses:
                                print(accesstoadd)

                            # Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference
                            spids = getSPIDs(rowafter.Users, rowafter.Groups)
                            # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                            hdfsentries = rowafter.Resources.strip(
                                "path=[").strip("]").split(",")
                            new_policy_tasks = []
                                        
                            for hdfsentry in hdfsentries:
                                print('calling bulk set')
                                new_policy_tasks.append(asyncio.create_task(setADLSBulkPermissions(storagetoken, spids, hdfsentry.strip(), permstr)))

                            startTimer = perf_counter()                    
                            await asyncio.wait(new_policy_tasks)                    
                            stopTimer = perf_counter()
                            print(f'New Policies tasks completed in {stopTimer-startTimer:.3f}')

                    else:
                        print("Changes were identified but no action taken. This could be due to: \n- A policy entry was updated but the fields stayed the same, just the order of the entities changed. \n- A change occured that did not pertain to paths, users, groups, permissions. \nThese change have been ignored.")
                else:
                    print("No other changes to process.")

        now = datetime.datetime.utcnow()
        progendtime = now.strftime('%Y-%m-%d %H:%M:%S')

        # save checkpoint in control table
        set_ct_info = "insert into " + dbname + "." + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + \
            appname + "', current_timestamp,'" + progstarttime + "','" + progendtime + \
            "'," + str(policy_rows_changed) + "," + str(acl_change_counter)+")"
        # print(set_ct_info)
        cursor.execute(set_ct_info)

    except pyodbc.DatabaseError as err:
        cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        print(
            'Error occured while processing file. Rollback. Error message: '.join(sqlstate))
    else:
        cnxn.commit()
        print('Done')
    finally:
        print('attempting to call cnxn.autocommit = True')
        # if (cnxn is not None):
        cnxn.autocommit = True

    # fs_client = service_client.get_file_system_client(adlpath)
    # dir_client = fs_client.get_directory_client(adlpath)
    # dir_client.set_access_control(None, None, permissions, spntype)

    basestorageuri = 'https://baselake.dfs.core.windows.net/base'
    spnaccsuffix = ''
    #print(spn + '-' + adlpath)
    # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    # Setup the endpoint
    puuid = str(uuid.uuid4())
    #print('Log analytics UUID'+ puuid)
    headers = {'x-ms-version': '2019-12-12', 'Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype+':'+spn+spnaccsuffix +
               ':'+permissions+',default:'+spntype+':'+spn+spnaccsuffix + ':'+permissions, 'x-ms-client-request-id': '%s' % puuid}
    #headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype+':'+spn+spnaccsuffix +',default:'+spntype+':'+spn+spnaccsuffix ,'x-ms-client-request-id': '%s' % puuid}
    request_path = basestorageuri+adlpath + \
        "?action=setAccessControlRecursive&mode=modify"
    print("Setting " + permissions + " ACLs for " +
          spntype + " " + spn + " on " + adlpath + "...")
    t1_start = perf_counter()
    r = requests.patch(request_path, headers=headers)
    response = r.json()
    t1_stop = perf_counter()
    # print(r.text)
    print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: " + str(
        response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: " + str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")
    return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))

# a variation of the function above which access a dictionary object of users and groups so that we can set the ACLs in bulk with a comma seprated list of ACEs (access control entries)


async def setADLSBulkPermissionsWithTasks(aadtoken, spids, adlpath, permissions, counter):

    async def logWrapper(adlpath, acl, counter):
        counter += await aclService.override_bulk_recursivly(adlPath=adlpath, acl=acl)

    tasks = []
    acentry = ""

    for sp in spids:

        for spid in spids[sp]:

            missingBit = 'user::r--,group::r--,other::r--'

            # +',default:'+sp+':'+spid + ':'+permissions +missingBit
            acentry = sp+':'+spid + ':'+permissions + ','+missingBit

            print(adlpath + ' -> '+acentry)

            tasks.append(asyncio.create_task(logWrapper(
                adlpath=adlpath, acl=acentry, counter=counter)))

    startTimer = perf_counter()
    await asyncio.wait(tasks)
    stopTimer = perf_counter()

    print(f'Completed in {stopTimer-startTimer:.3f}')

    return (counter)


async def setADLSBulkPermissions(aadtoken, spids, adlpath, permissions):

    acentry = ""

    for sp in spids:

        for spid in spids[sp]:

            missingBit = 'user::r--,group::r--,other::r--'

            # +',default:'+sp+':'+spid + ':'+permissions +missingBit
            acentry = sp+':'+spid + ':'+permissions + ','+missingBit

            print(adlpath + ' -> '+ acentry)

            await aclService.override_bulk_recursivly(adlPath=adlpath, acl=acentry)



def removeADLSBulkPermissions(aadtoken, spids, adlpath):
    # no permissions str required in a remove call
    acentry = ""
    for sp in spids:
        # print(spids[sp])
        for spid in spids[sp]:
            #print("Preparing " + sp + ' permissions for ' + spid)
            acentry += sp+':'+spid + ',default:'+sp+':'+spid + ','
    acentry = acentry.rstrip(',')
    basestorageuri = 'https://baselake.dfs.core.windows.net/base'
    spnaccsuffix = ''
    #print(spn + '-' + adlpath)
    # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    # Setup the endpoint
    puuid = str(uuid.uuid4())
    #print('Log analytics UUID'+ puuid)
    headers = {'x-ms-version': '2019-12-12', 'Authorization': 'Bearer %s' % aadtoken,
               'x-ms-acl': acentry, 'x-ms-client-request-id': '%s' % puuid}
    request_path = basestorageuri+adlpath + \
        "?action=setAccessControlRecursive&mode=remove"
    print("Removing ACLs: " + acentry + " on " + adlpath + "...")
    t1_start = perf_counter()
    r = requests.patch(request_path, headers=headers)
    response = r.json()
    t1_stop = perf_counter()

    if r.status_code == 200:
        print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: " + str(
            response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: " + str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")
    else:
        print("Error: " + str(r.text))
    return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
    #print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")

    basestorageuri = 'https://baselake.dfs.core.windows.net/base'
    spnaccsuffix = ''
    #print(spn + '-' + adlpath)
    # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    # Setup the endpoint
    puuid = str(uuid.uuid4())
    #print('Log analytics UUID'+ puuid)
    headers = {'x-ms-version': '2019-12-12', 'Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': spntype +
               ':'+spn+spnaccsuffix + ',default:'+spntype+':'+spn+spnaccsuffix, 'x-ms-client-request-id': '%s' % puuid}
    request_path = basestorageuri+adlpath + \
        "?action=setAccessControlRecursive&mode=remove"
    print("Removing " + permissions + " ACLs for " +
          spntype + " " + spn + " on " + adlpath + "...")
    t1_start = perf_counter()
    r = requests.patch(request_path, headers=headers)
    response = r.json()
    t1_stop = perf_counter()
    # print(r.text)
    print("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: " + str(
        response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: " + str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")
    return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))


def getBearerToken(resourcetype, spnid, spnsecret):

    
    #print('Graph API Token for Me: ' + graphApi.getToken(spnid, spnsecret))

    endpoint = 'https://login.microsoftonline.com/9388ab28-9d74-4bf6-9cb0-cdfcbbb3cbcb/oauth2/token'
    #endpoint = 'https://login.microsoftonline.com/'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'grant_type=client_credentials&client_id='+spnid + \
        '&client_secret=' + spnsecret + '&resource=https%3A%2F%2F'+resourcetype+'%2F'
    r = requests.post(endpoint, headers=headers, data=payload)
    response = r.json()
    print("Obtaining AAD bearer token for resource " + resourcetype + "...")

    bearertoken = response["access_token"]
    # print(bearertoken)
    print("Bearer token obtained.\n")
    return bearertoken


#################################################
#                                               #
#                    Main                       #
#                                               #
#################################################


async def main(mytimer: func.TimerRequest, starter:str) -> None:
    print('applyPolicies function starting...')

    if mytimer.past_due:
        print('The timer is past due!')

    try:        
        #tasks = [asyncio.create_task(getPolicyChanges())]
        #await asyncio.wait(tasks)
        #      
        await getPolicyChanges(starter)
        
    except Exception as e:
        print("Exceptions in async tasks: {}".format(str(e)))
    

    print('applyPolicies function finished.')