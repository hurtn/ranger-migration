#MIT License
#
#Copyright (c) 2021 Nick Hurt
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
#
# Policy change types
# 1 - new policy
# 2 - deleted policy *
# 3 - modification: policy enabled
# 4 - modification: policy disabled *
# 5 - modification: remove principals *
# 6 - modification: add principals
# 7 - modification: remove accesses/permissions * 
# 8 - modification: add accesses
# 9 - modification: remove paths *
# 10 - modification: add paths 
#
# * means these transaction types need to be validated against business rules e.g. rule of maximum

import os
import datetime
import logging
import urllib
import pyodbc
import pandas as pd
import pandas.io.common
import ast
from sqlalchemy import create_engine
from sqlalchemy import event
from tabulate import tabulate
import sqlalchemy
import azure.functions as func
import requests,uuid
from requests.auth import HTTPBasicAuth
from time import perf_counter 
from collections import defaultdict
import json
import typing

def main(mytimer: func.TimerRequest, msg: func.Out[typing.List[str]]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    devstage=os.environ["stage"]
    getPolicyChanges()
    businessRuleValidation()
    storeQueueItems(msg)
   

def getPolicyChanges():

    def aclErrorLogic(aclCount):
        if aclCount == 0:  errorflag = 1 # if no ACLs were ever set then enable the error flag
        if aclCount < 0: errorflag = aclCount  # any other reason may not necessarily be an error but either the stage variable was set to non-live or all users/groups were excluded


    # Obtains a list of security princpals IDs. Calls getSPID which fetches the ID from AAD
    def getSPIDs(userslist, groupslist):
        global exclusionCount
        global allPrincipalsExcluded
        global excludedPrincipals
        global principalsIncluded
        exclusionCount=0
        excludedPrincipals=[]
        principalsIncluded = defaultdict(list)
        # Local function to removes principals from the list of principals found in the policy permmaplist.
        def applyExclusions(vEntity,vExcludeEntities):
            global exclusionCount
            global excludedPrincipals

            #exlusionlist = vExcludeEntities.split(',')
            logging.info('Looking for exclusion: '+vEntity)
            if vEntity:
                for entity in vExcludeEntities:
                    if vEntity.lower() == entity.lower():
                        logging.info("Principal " + vEntity.lower() + " found on exclusions list, therefore ignoring...")  
                        exclusionCount+=1
                        excludedPrincipals.append(vEntity)
                        return None
                    else:
                        logging.info("Principal " + vEntity + " not found on exclusions list") 
                ##search for rule of maximum ie another policy which overrides this policy action
                return vEntity

        spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL
        totalValidUsers = 0
        totalValidGroups = 0
        # iterate through the comma separate list of groups and set the dictionary object
        if userslist is not None and len(userslist)>0:
            totalValidUsers = len(userslist)
            logging.info(str(userslist))
            userentries = str(userslist).split(",")
            for userentry in userentries:
                #logging.info("user: "+userentry.strip("['").strip("']").strip(' '))
                logging.info('user entry before '+ userentry)
                userentry = userentry.strip("[").strip("]").strip("'").strip(' ').strip("'")
                logging.info('Obtaining user ID: '+userentry)
                if userExclusionsList:
                  userentry = applyExclusions(userentry, userExclusionsList)
                if userentry:                
                    principalsIncluded['userList'].append(userentry)
                    spnid = getSPID(graphtoken,userentry,'users')
                    if spnid is not None:
                        spids['user'].append(spnid)

        # iterate through the comma separate list of groups and set the dictionary object
        if groupslist is not None and len(groupslist)>0:
            totalValidGroups = len(groupslist)
            groupentries = str(groupslist).split(",")
            for groupentry in groupentries:
                groupentry = groupentry.strip("[").strip("]").strip("'").strip(' ').strip("'")
                if groupExclusionsList:
                  groupentry = applyExclusions(groupentry, groupExclusionsList)
                if groupentry:                
                    principalsIncluded['groupList'].append(groupentry)
                    spnid = getSPID(graphtoken,groupentry,'groups')
                    if spnid is not None:
                      spids['group'].append(spnid)

        logging.info(str(exclusionCount) + '=' + str(totalValidGroups) + ' + ' + str(totalValidUsers))
        if (exclusionCount == totalValidGroups + totalValidUsers):
          logging.info('No remaining principals left as they were all matched to the exclusion list')
          allPrincipalsExcluded = 1
        else:
          allPrincipalsExcluded = 0
        return spids
   
    def captureTransaction(cursor,transaction,transmode, adlpath, spids, pPolicyID, lpermstr, ptranstype, permmap):
        global allPrincipalsExcluded
        #global userExclusionsList
        #global groupExclusionsList


        transStatus ='Validate' # assume transaction is going to be executed until it fails one of the validation steps
        transReason = '' # valid until proven otherwise
        request_path =''
        http_path = ''
        if (transmode=='modify' and spids and lpermstr) or (transmode=='remove' and spids): # only obtain the ACE entry if the parameters are valid for that specific transmode ie. modify needs both spids and perms and remove only needs spids
            acentry = spidsToACEentry(spids,lpermstr)
        else:
            acentry = ''

        if acentry is None or acentry =='':
            transStatus = 'Ignored'

            if allPrincipalsExcluded == 1:
              transReason = 'All principals excluded. Principals excluded: '+ ','.join(excludedPrincipals) + '.Permissions: '+lpermstr
              logging.info("No permissions could be set as all principals were on the exclusion list! Principals excluded: "+ ",".join(excludedPrincipals) + ".Permissions: "+lpermstr)    
            else:
              transReason = 'No AAD principals supplied. Check previous error messages but they may not have been found in AAD. Permissions string was '+lpermstr  
              logging.warning("No permissions could be set as access control entry is invalid, either does not contain principals or permissions!")    

        else:
            if len(excludedPrincipals)>0:
              transReason = 'Some principals were excluded due to the exclusion list. Principals excluded: '+ ','.join(excludedPrincipals) + '.Permissions: '+lpermstr
              logging.info("Some principals were on the exclusion list! Principals excluded: "+ ",".join(excludedPrincipals) + ".Permissions: "+lpermstr)    


        if adlpath is None:
            transStatus = 'Ignored'
            transReason = 'ADLS path is null'
            logging.error("No storage path obtained for policy "+ str(row.id)+": " + row.Name)
        else:
            if adlpath.find("abfs")>=0: # path contains full storage endpoint and resource therefore need to rearrange this 
                pathstr = adlpath.lstrip("['").rstrip("']")
                pathstr = pathstr.replace("abfs://","")
                urlparts = pathstr.split("@")
                contname = urlparts[0]
                pathparts = urlparts[1].split("/",1)
                accname = pathparts[0]
                tgtpath = pathparts[1]
                storageuri = ''
                http_path = 'https://'+accname+'/'+contname+'/'+tgtpath
                logging.info("Storage path set to "+http_path)
            if adlpath.find("hdfs")>=0:
                # if this is the default hive warehouse location then transform into the ADLS location
                http_path =  adlpath[adlpath.find("/warehouse")+10:]
                storageuri = basestorageuri
            else:
                storageuri = basestorageuri
            request_path = storageuri+http_path

        captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
        transinsert = "insert into " + dbschema + ".policy_transactions (policy_id, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason, all_principals_excluded,principals_excluded,exclusion_list,principals_included, adl_permission_str, permission_json) " \
                      " values ('" + str(pPolicyID) + "','" + request_path  + "','" + adlpath + "','" + transaction + "','" + transmode + "','" + acentry + "','"+ captureTime + "','" + str(ptranstype) + "','" + transStatus + "','" + transReason + "'," +str(allPrincipalsExcluded) + ",'" \
                      "" + ','.join(excludedPrincipals)+"','"+','.join(userExclusionsList)+','.join(groupExclusionsList)+"','" + json.dumps(principalsIncluded) +"','" +  lpermstr + "','" + json.dumps(permmap) + "')"
        logging.info("Capturing transaction: "+transinsert)
        cursor.execute(transinsert)


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

    # Removes duplicate entities
    def removeDups(vstring):
    
      def unique_list(l):
          ulist = []
          [ulist.append(x) for x in l if x not in ulist]
          return ulist
      if vstring:
        unique=' '.join(unique_list(vstring.split()))
        return unique
      else:
        return  ''    


    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    tenantid=os.environ["tenantID"]
    spnid= os.environ["SPNID"]
    spnsecret= os.environ["SPNSecret"]
    basestorageuri = os.environ["basestorageendpoint"]
    userExclusionsList = []
    groupExclusionsList = []
    errorflag=0
    #allPrincipalsExcluded = 0
    exclusionCount=0
    #excludedPrincipals=[]
    cnxn = pyodbc.connect(connxstr)

    try:
            # configure database params
            # connxstr=os.environ["DatabaseConnxStr"]
            appname = 'applyPolicies'
            targettablenm = "ranger_policies"
            batchsize = 200
            params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            #collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            #cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')

            # fetch exclusion list
            sql_txt = "select * from " + dbschema + ".exclusions where type in ('G','U');"
            logging.info(connxstr)
            logging.info(sql_txt)
            cursor.execute(sql_txt)
            row = cursor.fetchone()
            while row:
                if row[1] == 'U':
                  userExclusionsList.append(str(row[2]))
                if row[1] == 'G':
                  groupExclusionsList.append(str(row[2]))  
                row = cursor.fetchone()

            # note about the convert statements below, this is merely to convert the time value into the correct format for the fn_cdc_map_time_to_lsn function.
            # either there will be no data in the ctl table (first run) and then all changes are scanned. otherwise there is a last checkpoint found. if this matches the maximum lsn of the database then no changes have happened since the last run ie do nother. otherwise scan for changes...
            get_ct_info = "select convert(varchar(23),lsn_checkpoint,121) checkpointtime,convert(varchar(23),sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()),121) maxlsntime from " + dbschema + ".policy_ctl where id= (select max(id) from " + dbschema + ".policy_ctl where application = '" + appname +"');"
            logging.info(get_ct_info)
            logging.info("Getting control table information...")
            cursor.execute(get_ct_info)
            row = cursor.fetchone()
            lsn_checkpoint=None
            max_lsn_time=None
            acl_change_counter = 0
            acls_changed = 0
            policy_rows_changed =0

            if row:
                logging.info("Last checkpoint was at "+str(row[0]) +". Max LSN time is "+ str(row[1]))
                lsn_checkpoint  = row[0]
                max_lsn_time = row[1]
                ignoreTransaction = 0 # assume transaction is going to be executed until it fails one of the validation steps
                if lsn_checkpoint == max_lsn_time:
                    logging.info("Database has not increased LSN since last check. Sleeping...")
                    now =  datetime.datetime.utcnow()
                    progendtime = now.strftime('%Y-%m-%d %H:%M:%S')
                    set_ct_info = "insert into " + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + appname + "','" + progstarttime + "','"+ progendtime + "','" + max_lsn_time + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
                    #logging.info(set_ct_info)
                    cursor.execute(set_ct_info)
                    # now terminate the program
                    return


                
            else: 
              logging.info("No control information, obtaining all changes...")
              

            # changessql is the string variable which holds the SQL statements to determine the changes since the last checkpoint   
            changessql = "DECLARE  @from_lsn binary(10), @to_lsn binary(10); " 
            # comment this if statement if you wish to retreive all rows every time
            if lsn_checkpoint is not None and max_lsn_time is not None:
              changessql = changessql + """SET @from_lsn =sys.fn_cdc_map_time_to_lsn('smallest greater than','""" + str(lsn_checkpoint) + """')
                                        SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal','""" + str(max_lsn_time) + """')""" ## sys.fn_cdc_get_max_lsn() """

            else: 
                changessql = changessql + """SET @from_lsn =sys.fn_cdc_get_min_lsn('dbo_ranger_policies');
                                                SET @to_lsn = sys.fn_cdc_get_max_lsn(); """
                   #cursor.execute("select sys.fn_cdc_get_min_lsn('dbo_ranger_policies'), sys.fn_cdc_get_max_lsn()")
                   #row = cursor.fetchone()
                   #start_lsn = row[0]
                   #end_lsn = row[1]
                   #cursor.cancel() 
            changessql = changessql + """            
            select [__$operation],[id],[Name],coalesce(resources,paths) Resources,[Status],replace(permMapList,'''','"') permMapList,[Service Type],tables,table_type,table_names 
            from cdc.fn_cdc_get_all_changes_""" + dbschema + """_""" + targettablenm  + """(@from_lsn, @to_lsn, 'all update old') 
            order by id,__$seqval,__$operation;"""

            logging.info(changessql)
            # determine if last checkpoint is available and if so read all the changes into a pandas dataframe
            changesdf= pandas.io.sql.read_sql(changessql, cnxn)
            changesdf = changesdf[(changesdf['Service Type']=='hive')]
            logging.info(changesdf)
            if changesdf.empty:

              logging.info("No changes found. Exiting...")
              ## TODO This needs to be reworked because we should still update the checkpoint if no rows were found
              
              now =  datetime.datetime.utcnow()
              progendtime = now.strftime('%Y-%m-%d %H:%M:%S')
              if max_lsn_time is None:
                  max_lsn_time = progendtime
              # save checkpoint in control table
              set_ct_info = "insert into " + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + appname + "','" + progstarttime + "','"+ progendtime + "','" + max_lsn_time + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
              logging.info('Saving checkpoint after no changes found: ' + set_ct_info)
              cursor.execute(set_ct_info)
              # now terminate the program
              return
            else:
               uniquepolicyids = changesdf.groupby('id').id.nunique()
               policy_rows_changed  =[uniquepolicyids.value_counts()][0][1] # get the number of unique policy ids in the changed record set which will be stored on the control table at the end of the routine
               logging.info("Number of unique policy records changed: " + str(policy_rows_changed))
               

            # CDC operation 1 = deleted record
            deleteddf = changesdf[(changesdf['__$operation']==1)]
            # CDC operation 2 = inserted record, filter by new policies entries
            insertdf = changesdf[(changesdf['__$operation']==2)]
            # CDC operation 3 is the before image of the row, operation 4 is the after (current) image.
            updatesdf = changesdf[(changesdf['__$operation']==3) |(changesdf['__$operation']==4)]

            if not (insertdf.empty and updatesdf.empty and deleteddf.empty): # if there are either inserts or updates then only get tokens
                #storagetoken = getBearerToken(tenantid,"storage.azure.com",spnid,spnsecret)
                graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)

            #################################################
            #                                               #
            #                 New Policies                  #
            #                                               #
            ################################################# 

            if not insertdf.empty:
                #there are changes to process. first obtain an AAD tokens

                logging.info("\nNew policy rows detected:")
                logging.info(insertdf)
                logging.info("\n")

                # iterate through the new policy rows
                for row in insertdf.loc[:, ['__$operation','id','Name','Resources','Status','permMapList','Service Type','table_type','tables','table_names']].itertuples():
                    
                    if row.Status in ('Enabled','True') : 

                        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                        hdfsentries = row.Resources.strip("path=[").strip("[").strip("]").split(",")

                        #Load the json string of permission mappings into a dictionary object
                        permmaplist = json.loads(row.permMapList)
                        tableNames = json.loads(row.table_names)

                        for permap in permmaplist: #this loop iterates through each permMapList and applies the ACLs
                            for perm in permap["permList"]:
                                logging.info("Permission: "+perm)
                            # determine the permissions rwx
                            permstr = getPermSeq(permap["permList"])    
                            logging.info("perm str is now "+permstr)
                            permstr = permstr.ljust(3,'-')
                            logging.info("Permissions to be set: " +permstr)
                            for groups in permap["groupList"]:
                                logging.info("Groups: " + groups)
                            for userList in permap["userList"]:
                                logging.info("Users: " + userList)

                            # obtain a list of all security principals
                            spids = getSPIDs(permap["userList"],permap["groupList"])

                            if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                              logging.warning("***** Table exclusion list in policy detected")
                              tablesToExclude = row.tables.split(",")
                              # iterate through the array of tables for this database
                              for tblindb in tableNames:
                                  isExcluded = False  # assume not excluded until there is a match
                                  for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                      logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                      if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                          isExcluded = True
                                          logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                  if not isExcluded:
                                    logging.warning("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNames[tblindb])  
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', tableNames[tblindb],spids,row.id,permstr,1,permap["permList"])

                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            else: #capture entry as normal at the database level
                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    logging.info("Passing path: " + hdfsentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', hdfsentry,spids,row.id,permstr,1,permap["permList"])

            elif not deleteddf.empty:
            #################################################
            #                                               #
            #               Deleted Policies                #
            #                                               #
            ################################################# 


                logging.info("\nDeleted policy rows detected:")
                logging.info(deleteddf)
                logging.info("\n")

                # iterate through the deleted policy rows
                for row in deleteddf.loc[:, ['__$operation','id','Name','Resources','Status','permMapList','Service Type','table_type','tables','table_names']].itertuples():
                    
                    if row.Status in ('Enabled','True'): # only bother deleting ACLs where the policy was in an enabled state

                        #Load the json string of permission mappings into a dictionary object
                        permmaplist = json.loads(row.permMapList)
                        tableNames = json.loads(row.table_names)

                        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                        hdfsentries = row.Resources.strip("path=[").strip("[").strip("]").split(",")

                        for permap in permmaplist: #this loop iterates through each permMapList and applies the ACLs
                            # no need to determine the permissions in a delete/remove ACL scernario
                            ##permstr = getPermSeq(row.Accesses.split(","))    
                            
                            # if table_type == 'Exclusion' and tables != '*':
                              # iterate through array of tables for this database
                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            # else: capture entry as normal at the database level

                            spids = getSPIDs(permap["userList"],permap["groupList"])

                            if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                              logging.info("***** Table exclusion list in policy detected")
                              tablesToExclude = row.tables.split(",")
                              # iterate through the array of tables for this database
                              for tblindb in tableNames:
                                  isExcluded = False  # assume not excluded until there is a match
                                  for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                      logging.info("Comparing " +  tblToExclude + " with " + tblindb)
                                      if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                          isExcluded = True
                                          logging.info("***** Table " + tblindb + " is to be excluded from ACLs")
                                  if not isExcluded:
                                    logging.info("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNames[tblindb])  
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', tableNames[tblindb],spids,row.id,'',2,'')


                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            else: #capture entry as normal at the database level

                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                    logging.info("Removing ACLs from deleted policy for path: " + hdfsentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', hdfsentry,spids,row.id,'',2,'')

            else:
                logging.info("No new or deleted policies detected. ")

            logging.info("Determining any other changes...")

            #################################################
            #                                               #
            #             Modified Policies                 #
            #                                               #
            ################################################# 

            if not updatesdf.empty:
                logging.info("\nModified policy rows detected:")
                logging.info(updatesdf)
                logging.info("\n")

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
                aclsForAllPathsSet = False #this variable is an modified policy optimisation - for example if the changes included a new permisssion and simultaneously a new path/database was added there is no need to do these both as there would be duplication so we set a flag when one is done to avoid doing the other

                # Comment: 11b
                # Fetch the first and last row of changes for a particular ID. This is because we are only concerned with the before 
                # and after snapshot, even if multiple changes took place. e.g. if a complex scenario arises in one run such as removal of users/groups, addition of new users/groups, as well as changes to permissions or paths,
                # this is handled as a recursive deletion of these users/groups from the ACLs from the previous image of the path (in case this was part of the changed fields), and then the new users/groups are recursively added to the latest image of the path
                firstandlastforid = updatesdf[(updatesdf['id']==rowid)].iloc[[0, -1]]

                if row['Service Type']=='hive':
                    #resourcesbefore = firstandlastforid.get(key = 'Resources')[0].split(",")
                    #print("Resource changes....")
                    #print(firstandlastforid.get(key = 'Resources'))
                    #print(tabulate(updatesdf, headers='keys', tablefmt='presto'))
                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]] is not None:
                        resourcesbefore = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]])
                    else:
                        resourcesbefore = ''
                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]] is not None:
                        resourcesafter = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]])
                    else:
                        resourcesafter =  ''
                    if  firstandlastforid.get(key = 'table_names')[firstandlastforid.head(1).index[0]] is not None:
                        tableNamesBefore = json.loads(firstandlastforid.get(key = 'table_names')[firstandlastforid.head(1).index[0]])
                    else:
                        tableNamesBefore = ''

                    if firstandlastforid.get(key = 'table_names')[firstandlastforid.tail(1).index[0]] is not None:
                        tableNamesAfter = json.loads(firstandlastforid.get(key = 'table_names')[firstandlastforid.tail(1).index[0]])
                    else:
                        tableNamesAfter = ''
                    
                elif row['Service Type']=='hdfs':
                    resourcesbefore = firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]].split(",")
                    resourcesafter = firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]].split(",")   # note the syntax [firstandlastforid.tail(1).index[0]] fetches the index of the last record in case there were multiple changes
                else:
                    resourcesbefore = ''
                    resourcesafter = ''

                logging.info("Resources before: "+ str(resourcesbefore))
                logging.info("Resources after: " + str(resourcesafter))

                statusbefore = firstandlastforid.get(key = 'Status')[firstandlastforid.head(1).index[0]].strip()
                statusafter = firstandlastforid.get(key = 'Status')[firstandlastforid.tail(1).index[0]].strip()

                # load the permMapList into a json aray
                permMapBefore = json.loads(firstandlastforid.get(key = 'permMapList')[firstandlastforid.head(1).index[0]])
                permMapAfter = json.loads(firstandlastforid.get(key = 'permMapList')[firstandlastforid.tail(1).index[0]])

                # obtain the length of each array and then get the max - this will be the number of iterations that we are comparing for changes. any index that is out of bounds is essentially a delete operation because it has been removed
                maplistcountbefore = len(permMapBefore)
                maplistcountafter = len(permMapAfter)

                maplistmax = max(maplistcountbefore,maplistcountafter)
                logging.info("maplistcount before: "+str(maplistcountbefore) + " after:" + str(maplistcountafter) + " max "+ str(maplistmax))
                logging.info("permmapbf: " + str(permMapBefore))
                logging.info("permmapaf: " + str(permMapAfter))

                # obtain table type (exclusion or inclusion) setting and tables if any
                tableTypeBefore =  firstandlastforid.get(key = 'table_type')[firstandlastforid.head(1).index[0]].strip()
                tableTypeAfter = firstandlastforid.get(key = 'table_type')[firstandlastforid.tail(1).index[0]].strip()
                if firstandlastforid.get(key = 'tables')[firstandlastforid.head(1).index[0]] is not None:
                    tableListBefore =  firstandlastforid.get(key = 'tables')[firstandlastforid.head(1).index[0]].strip("[").strip("]").split(",")
                else:
                    tableListBefore=''
                if firstandlastforid.get(key = 'tables')[firstandlastforid.tail(1).index[0]] is not None:
                    tableListAfter = firstandlastforid.get(key = 'tables')[firstandlastforid.tail(1).index[0]].strip("[").strip("]").split(",")
                else:
                    tableListAfter=''
                if tableTypeBefore != tableTypeAfter and tableTypeAfter == 'Exclusion' and tableListAfter[0] != "*": 
                    tableExclusionSet = True
                else: 
                    tableExclusionSet=False

                if (tableTypeBefore != tableTypeAfter and tableTypeAfter == 'Inclusion') or tableListAfter[0] == "*": 
                    tableExclusionRemoved = True
                else: 
                    tableExclusionRemoved =False


                # iterate through the permaplist array
                for n in range(0,maplistmax):
                    accessesbefore = ''
                    groupsbefore = ''
                    usersbefore = ''
                    usersafter =''
                    
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
                      usersafter = ''
                    try:
                        # determine perms, group and user after images
                        accessesafter = permMapAfter[n]["permList"]
                        for perm in accessesafter:
                                logging.info("Permission: "+perm)
                            # determine the permissions rwx

                        permstr = getPermSeq(accessesafter)    
                        permstr = permstr.ljust(3,'-')
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
                    logging.info(str(n) + " - groups before = " + str(groupsbefore) + ". Groups after " + str(groupsafter)) 
                    logging.info(str(n) + " - users before = " + str(usersbefore) + ". Users after " + str(usersafter)) 
                    addgroups = entitiesToAdd(groupsbefore,groupsafter)
                    removegroups = entitiesToRemove(groupsbefore,groupsafter)    
                    addusers = entitiesToAdd(usersbefore,usersafter)
                    removeusers = entitiesToRemove(usersbefore,usersafter)    

                    #check if they are really different even if the order was simply changed
                    if addgroups or removegroups:
                        if check_if_equal(addgroups, removegroups):
                            logging.info('Groups in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                            addgroups = None
                            removegroups = None

                    if addusers or removeusers:
                        if check_if_equal(addusers, removeusers):
                            logging.info('Users in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
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
                            logging.info('Policy now enabled, same as a new policy - add ACLS')    

                            
                            # obtain a dictionary list of all security principals
                            spids = getSPIDs(usersafter,groupsafter)

                            # if table_type == 'Exclusion' and tables != '*':
                              # iterate through array of tables for these databases
                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            # else: capture entry as normal at the database level

                            # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                            for resourceentry in resourcesafter:
                                resourceentry = resourceentry.strip().strip("'")
                                logging.info("Passing path: " + resourceentry)
                                captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,3,accessesafter)


                        if statusafter not in ('Enabled','True'): # a disabled policy is treated in the same way as a deleted policy
                            logging.info('Policy now disabled therefore delete ACLs')  

                            # if table_type == 'Exclusion' and tables != '*':
                              # iterate through array of tables for these databases
                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            # else: capture entry as normal at the database level

                            for resourceentry in resourcesafter:
                                resourceentry = resourceentry.strip().strip("'")
                                # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                spids = getSPIDs(usersafter,groupsafter)
                                logging.info("Passing path: " + resourceentry)
                                captureTransaction(cursor,'setAccessControlRecursive','remove', resourceentry,spids,row.id,'',4,accessesafter)


                    elif statusafter in ('Enabled','True'): # if there wasn't a status change, then only bother dealing with modifications is the policy is set to enabled i.e. we don't care about policies that were disabled prior to the change and are still not enabled. when / if they are eventually enabled they will be treated as any new policy would

                        # check for new or modified hive objects which will result in a change in paths to add ACLs to. 
                        addresources = entitiesToAdd(resourcesbefore,resourcesafter)
                        removeresources = entitiesToRemove(resourcesbefore,resourcesafter)   

                        # process incremental changes to groups, users, accesses or resources if there wasn't a status change above which would take care of these for a specific policy
                        if addgroups or addusers or removegroups or removeusers or removeaccesses or addaccesses or removeresources or addresources or tableExclusionSet or tableExclusionRemoved: 

                            if tableExclusionSet:
                                #now remove just the ACLs for the paths of the excluded tables
                                None
                        
                            if tableExclusionRemoved:
                                #now add just the ACLs for the paths of the tables in the before image of the record
                                None

                            #########################
                            # user or group changes
                            ##########################
                            if removegroups or removeusers:
                                if removegroups:
                                    logging.info("Remove the following groups: ")
                                    for grouptoremove in removegroups:
                                        logging.info(grouptoremove)
                                if removeusers:
                                    logging.info("Remove the following users: ")
                                    for usertoremove in removeusers:
                                        logging.info(usertoremove)


                            # if table_type == 'Exclusion' and tables != '*':
                              # iterate through array of tables for these databases
                              # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                            # else: capture entry as normal at the database level
                                for resourceentry in resourcesbefore:
                                    resourceentry = resourceentry.strip().strip("'")
                                    # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                    spids = getSPIDs(removeusers,removegroups)
                                    logging.info("Passing path: " + resourceentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', resourceentry,spids,row.id,'',5,accessesafter)                                    
                                    #if resourceentry:
                                    #    logging.info('calling bulk remove per directory path')
                                    #    acls_changed += removeADLSBulkPermissions(basestorageuri,storagetoken, spids,  resourceentry)
                                    #    acl_change_counter += acls_changed
                                    #    if acls_changed==0:
                                    #        errorflag=1
                                    ##else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))

                            if addgroups or addusers: 
                                if addgroups:
                                    logging.info("Add the following groups: ")
                                    for grouptoadd in addgroups:
                                        logging.info(grouptoadd)
                                if addusers:    
                                    logging.info("Add the following users")
                                    for usertoadd in addusers:
                                        logging.info(usertoadd)

                                spids = getSPIDs(addusers,addgroups)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference
                         
                                for resourceentry in resourcesafter:
                                    resourceentry = resourceentry.strip().strip("'")
                                    #logging.info('Were all principals excluded? ' + str(allPrincipalsExcluded))
                                    logging.info("Passing path: " + resourceentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,6,accessesafter)
                                    #if resourceentry:
                                    #    logging.info('calling bulk set')
                                    #    acls_changed += setADLSBulkPermissions(basestorageuri,storagetoken, spids, resourceentry, permstr)
                                    #    acl_change_counter += acls_changed
                                    #    if acls_changed==0:
                                    #        errorflag=1
                                    #else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))

                            #######################################
                            # determine access/permissions changes
                            ######################################
                            # note we do not actually need to calculate/apply the differences in permissions because 
                            # the way to apply permission changes for a principal is simply to apply the latest image of the permissions 
                            # i.e. can't incrementally add a Write, one needs to add the ACL entry again for a particular path and AAD OID
                            if removeaccesses:
                                logging.info("remove the following accesses")
                                for accesstoremove in removeaccesses:
                                    logging.info(accesstoremove)

                                spids = getSPIDs(usersafter,groupsafter)
                                for resourceentry in resourcesafter:
                                    resourceentry = resourceentry.strip().strip("'")
                                    logging.info("Passing path: " + resourceentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,7,removeaccesses)
                                    #if resourceentry:
                                    #    logging.info('calling bulk remove per directory path')
                                    #    acls_changed += setADLSBulkPermissions(basestorageuri,storagetoken, spids, resourceentry, permstr)
                                    #    aclErrorLogic(acls_changed)
                                    #    acl_change_counter += acls_changed
                                    #    if acls_changed==0:
                                    #        errorflag=1

                                    #else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))


                            if addaccesses:
                                logging.info("add the following accesses")
                                for accesstoadd in addaccesses:
                                    logging.info(accesstoadd)

                                spids = getSPIDs(usersafter,groupsafter)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference

                                for resourceentry in resourcesafter:
                                    resourceentry = resourceentry.strip().strip("'")
                                    logging.info("Passing path: " + resourceentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,8,addaccesses)
                                    #if resourceentry:
                                    #    #logging.info('calling bulk set')
                                    #    acls_changed += setADLSBulkPermissions(basestorageuri,storagetoken, spids, resourceentry, permstr)
                                    #    acl_change_counter += acls_changed
                                    ##    if acls_changed==0:
                                    #        errorflag=1
                                    #else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))


                            if removeresources:

                                spids = getSPIDs(usersbefore,groupsbefore)

                                for resourcetoremove in removeresources:
                                    resourcetoremove = resourcetoremove.strip().strip("'")
                                    logging.info("Removing ACLs from deleted directory path: " + resourcetoremove)
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', resourcetoremove,spids,row.id,'',9,accessesafter)
                                    #if resourcetoremove:
                                    #    logging.info(resourcetoremove)
                                    #    acls_changed += removeADLSBulkPermissions(basestorageuri,storagetoken, spids, resourcetoremove)
                                    #    acl_change_counter += acls_changed
                                    #    if acls_changed==0:
                                    #        errorflag=1
                                    #else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))

                            if addresources:
                                logging.info("add the new permissions to the following resources")
                                spids = getSPIDs(usersafter,groupsafter)
                                for resourcetoadd in addresources:
                                    resourcetoadd = resourcetoadd.strip().strip("'")
                                    logging.info("Adding ACLs to new directory path: " + resourcetoadd)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', resourcetoadd,spids,row.id,permstr,10,accessesafter)
                                    #if resourcetoadd:
                                    #    logging.info(resourcetoadd)
                                    #    acls_changed += setADLSBulkPermissions(basestorageuri,storagetoken, spids, resourcetoadd, permstr)
                                    #    acl_change_counter += acls_changed
                                    #    if acls_changed==0:
                                    #        errorflag=1
                                    #else:
                                    #    logging.warning("No storage path obtained for policy "+ str(rowid))


                        else:
                            #logging.info("Changes were identified but no action taken. This could be due to: \n- A policy entry was updated but the fields stayed the same, just the order of the entities changed. \n- A change occured that did not pertain to paths, users, groups, permissions. \nThese change have been ignored.")
                            logging.info("No valid changes found. No further action required")
                    else: 
                        logging.info("No other changes to process.")


            now =  datetime.datetime.utcnow()
            progendtime = now.strftime('%Y-%m-%d %H:%M:%S')
            if max_lsn_time is None:
                max_lsn_time = progendtime

            # save checkpoint in control table
            if errorflag<=0:
                if errorflag ==1:
                    logging.info("No principals were in the ACE entry. Transaction ignored. Checkpoint progressed.")
                if errorflag ==2:
                    logging.info("Stage environment variable set to non-prod therefore no ACLs were set and transaction ignore. Checkpoint progressed.")
                set_ct_info = "insert into " + dbschema + ".policy_ctl (application,start_run, end_run, lsn_checkpoint,rows_changed, acls_changed) values ('" + appname + "','" + progstarttime + "','"+ progendtime + "','" + max_lsn_time + "'," +str(policy_rows_changed) + "," + str(acl_change_counter)+")"
                cursor.execute(set_ct_info)
                logging.info('Saving checkpoint after run: ' + set_ct_info)
            else:
                logging.error("Error detected during processing. Please see previous warnings or errors above for more information. Checkpoint will not be saved. Retry will occur during next run.")
        

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            logging.info('Done')
    finally:
            cnxn.autocommit = True



# a variation of the function above which access a dictionary object of users and groups so that we can set the ACLs in bulk with a comma seprated list of ACEs (access control entries)
def setADLSBulkPermissions(storageuri, aadtoken, spids, adlpath, permissions):
    if spids:
        acentry = ""
        for sp in spids:
            #logging.info('SPID' + str(spids[sp]))
            for spid in spids[sp]:
                #logging.info("Preparing " + sp + ' permissions for ' + spid)
                acentry += sp+':'+spid+ ':'+permissions+',default:'+sp+':'+spid + ':'+permissions +','
        acentry = acentry.rstrip(',') 
        #logging.info("Setting permission: "+acentry)
        #logging.info(acentry.rstrip(','))
        if adlpath.find("abfs")>=0:
            pathstr = adlpath.lstrip("['").rstrip("']")
            pathstr = pathstr.replace("abfs://","")
            urlparts = pathstr.split("@")
            contname = urlparts[0]
            pathparts = urlparts[1].split("/",1)
            accname = pathparts[0]
            tgtpath = pathparts[1]
            storageuri = ''
            adlpath = 'https://'+accname+'/'+contname+'/'+tgtpath
            logging.info("Storage path set to "+adlpath)
        spnaccsuffix = ''
        # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
        puuid = str(uuid.uuid4())
        headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl':acentry,'x-ms-client-request-id': '%s' % puuid}
        request_path = storageuri+adlpath+"?action=setAccessControlRecursive&mode=modify"
        logging.info("Setting " + permissions + " ACLs  " + acentry + " on " +adlpath + "...")
        t1_start = perf_counter() 
        if devstage == 'live':
            r = requests.patch(request_path, headers=headers)
            response = r.json()
            t1_stop = perf_counter()
            #logging.info(r.text)
            if r.status_code == 200:
                logging.info("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
                return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
            else:
                logging.error("Error: " + str(r.text))
                return(0)
        else:
            return(-2)
    else:
        logging.warning("Warning: Could not set ACLs as no users/groups were supplied in the ACE entry. This can happen when all users are either in the exclusion list or their IDs could not be found in AAD.")    
        return(-1)
    #aces = spntype+':'+spn+spnaccsuffix + ':'+permissions+',default:'+spntype+':'+spn+spnaccsuffix + ':'+permissions,'x-ms-client-request-id': '%s' % puuid


def removeADLSBulkPermissions(storageuri,aadtoken, spids, adlpath):
    ## no permissions str required in a remove call
    acentry = ""
    if spids:
        for sp in spids:
            #logging.info(spids[sp])
            for spid in spids[sp]:
            #logging.info("Preparing " + sp + ' permissions for ' + spid)
              acentry += sp+':'+spid +',default:'+sp+':'+spid +','
        acentry = acentry.rstrip(',') 
        if adlpath.find("abfs")>=0:
            pathstr = adlpath.lstrip("['").rstrip("']")
            pathstr = pathstr.replace("abfs://","")
            urlparts = pathstr.split("@")
            contname = urlparts[0]
            pathparts = urlparts[1].split("/",1)
            accname = pathparts[0]
            tgtpath = pathparts[1]
            storageuri = ''
            adlpath = 'https://'+accname+'/'+contname+'/'+tgtpath
            logging.info("Storage path set to "+adlpath)

        spnaccsuffix = ''
        #logging.info(spn + '-' + adlpath)
        # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
        #Setup the endpoint
        puuid = str(uuid.uuid4())
        #logging.info('Log analytics UUID'+ puuid)
        headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl': acentry,'x-ms-client-request-id': '%s' % puuid}
        request_path = storageuri+adlpath+"?action=setAccessControlRecursive&mode=remove"
        logging.info("Removing ACLs: " + acentry + " on " +adlpath + "...")
        t1_start = perf_counter() 
        if devstage == 'live':
            r = requests.patch(request_path, headers=headers)
            response = r.json()
            t1_stop = perf_counter()

            if r.status_code == 200:
                logging.info("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
                return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
            else:
                logging.error("Error: " + str(r.text))
                return(0)
        else:
            return(-2)
        #logging.info("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
    else:
        logging.warning("Warning: Could not set ACLs as no users/groups were supplied in the ACE entry. This can happen when all users are either in the exclusion list or their IDs could not be found in AAD.")    
        return(-1)

def getSPID(aadtoken, spname, spntype):
    # Graph docs - Odata filter: https://docs.microsoft.com/en-us/graph/query-parameters#filter-parameter
    if spntype == 'users': odatafilterfield = "userPrincipalName"
    else: odatafilterfield = "displayName"
    spname = spname.strip().strip("'") #cleanup
    logging.info("AAD Directory look up for " + spntype + ": " + spname)
    headers ={'Content-Type': 'application/json','Authorization': 'Bearer %s' % aadtoken}
    request_str = "https://graph.microsoft.com/v1.0/"+spntype+"?$filter=startsWith("+odatafilterfield+",'"+spname.strip().replace('#','%23')+"')"
    #https://graph.microsoft.com/v1.0/users?$filter=startswith(userPrincipalName,'nihurt@microsoft.com')
    #logging.info(aadtoken)
    logging.info(request_str)
    r = requests.get(request_str, headers=headers)

    if r.status_code==200:
        response = r.json()
        #logging.info(response)
        if response["value"]:
            logging.info("Found OID " + response["value"][0]["id"])
            return response["value"][0]["id"]
        else:
            logging.info("Warning: Could not find user ID!!! Response: "+str(response))
            # at this point should we aboort the process or just log the failure?? TBD by client
            return None
    elif r.status_code==403:
        if spname.strip().replace('#','%23') == 'nick.hurt': return 'cb0c78ea-0032-411a-ae61-0c616d2560e8'
        if spname.strip().replace('#','%23') == 'aramanath': return '818c16bc-2ab3-41bd-bd7f-ea0124b931f0'
    else:
        logging.warning("Warning: Could not find user ID!!! Response: "+str(r.status_code) + ": "+r.text)
        # at this point should we aboort the process or just log the failure?? TBD by client
        return None


def spidsToACEentry(spids,permissions):
    aceentry = ""
    if spids:
        for sp in spids:
            #logging.info('SPID' + str(spids[sp]))
            for spid in spids[sp]:
                #logging.info("Preparing " + sp + ' permissions for ' + spid)
                if permissions:
                    aceentry += sp+':'+spid+ ':'+permissions+',default:'+sp+':'+spid + ':'+permissions +','
                else: # the specification to remove ACLs doesn't require a perm str, only the SPID(s)
                    aceentry += sp+':'+spid +',default:'+sp+':'+spid +','

        aceentry = aceentry.rstrip(',') 
        return aceentry
    else:
        return None

def getPermSeq(perms):
    lpermstr=''
    for perm in perms:
        logging.info('perm to lookup='+perm+'.')
        if perm.strip()  == 'read' and lpermstr.find('r')<0 : lpermstr='r'
        elif perm.strip() == 'select' and lpermstr.find('r')<0: lpermstr='r'
        elif perm.strip() == 'write' and lpermstr.find('r')<0 and lpermstr.find('w')<0: lpermstr='-w' # this is a special case where no read permissions were found
        elif perm.strip() == 'write' and lpermstr.find('r')>=0 and lpermstr.find('w')<0: lpermstr='rw'
        elif perm.strip() == 'update' and lpermstr.find('w')<0: lpermstr+='w'
        elif perm.strip() == 'execute' and lpermstr.find('x')<0: lpermstr+='x'
        elif perm.strip() == 'all': return 'rwx'
        else: lpermstr+=''
    logging.info('permstr to return='+lpermstr+'.')
    return lpermstr

def getBearerToken(tenantid,resourcetype,spnid,spnsecret):
    endpoint = 'https://login.microsoftonline.com/' + tenantid + '/oauth2/token'

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'grant_type=client_credentials&client_id='+spnid+'&client_secret='+ spnsecret + '&resource=https%3A%2F%2F'+resourcetype+'%2F'
    #payload = 'resource=https%3A%2F%2F'+resourcetype+'%2F'
    #logging.info(endpoint)
    #logging.info(payload)
    r = requests.post(endpoint, headers=headers, data=payload)
    response = r.json()
    logging.info("Obtaining AAD bearer token for resource "+ resourcetype + "...")
    try:
      bearertoken = response["access_token"]
    except KeyError:
      logging.info("Error obtaining bearer token: "+ response)
    #logging.info(bearertoken)
    logging.info("Bearer token obtained.\n")
    return bearertoken

def businessRuleValidation():
    tenantid=os.environ["tenantID"]
    spnid=os.environ["SPNID"]
    spnsecret= os.environ["SPNSecret"]
    spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL

    def entitiesToRemove(beforelist, afterlist):
        return (list(set(beforelist) - set(afterlist)))                    

    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)

    try:
            appname = 'applyPolicies'
            params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            cursor = cnxn.cursor()
            cursorinner = cnxn.cursor()
            transcursor = cnxn.cursor()
            #transfixcursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
            new_trans_status = 'Pending'
            # determine if there are any in flight transactions that should be aborted based on the fact that they are for the same policy id, but complete reversal of a transaction that is in progress
            trans_to_abort = """with trans as (
                                select id,policy_id, trans_type, adl_path, principalexp.value principalval, principals_included,storage_url, 'userList' principal_type, permission_json, trans_status from policy_transactions pt cross apply OPENJSON(principals_included) with (users nvarchar(max) '$.userList' as json) cross APPLY OPENJSON(users) principalexp)
                                select o.id,o.trans_status, t.id from trans t inner join trans o on t.policy_id = o.policy_id and t.trans_status = 'Validate' and o.trans_status = 'InProgress' and t.trans_type =5 and o.trans_type =1 and t.adl_path = o.adl_path and t.principalval =  o.principalval"""

            cursor.execute(trans_to_abort)
            transactions = cursor.fetchall()
            logging.info("Determine whether this is a policy correction...")
            if len(transactions) > 0:
                logging.info("Policy corrections detected...")
                for transaction_row in transactions:

                  trans_abort  = """update policy_transactions set trans_status = 'Abort', trans_reason= concat(trans_reason,'Transaction aborted as user error correction detected in transaction """+ str(transaction_row[2]) + """') where id = """ + str(transaction_row[0])
                  logging.info(trans_abort)
                  cursor.execute(trans_abort)
                  cnxn.commit() 
                  new_trans_status = 'Waiting'
                  
            #  determine which changes have matches to paths and principals elsewhere. if any fetch the paths, principals and policy ID(s) of the other policy/ies (in comma separated notation). The latter will be stored in the trans reason.
            # TODO add the inverse                                 union 
                                #select distinct t.id,t.policy_id, t.trans_type, t.adl_path,  t.principalval, t.principals_included, stuff((select distinct ',' + cast(ps.id as varchar) for xml path ('')),1,1,'') as policylist, t.storage_url,principal_type,permission_json from trans t inner join policy_snapshot_by_path ps on  ps.adl_path like t.adl_path+'%'  and t.principalval = ps.principal and t.policy_id <> ps.id
            policy_conflicts = """with trans as (
                                select id,policy_id, trans_type, adl_path, principalexp.value principalval, principals_included,storage_url, 'userList' principal_type, permission_json from policy_transactions pt cross apply OPENJSON(principals_included) with (users nvarchar(max) '$.userList' as json) cross APPLY OPENJSON(users) principalexp where trans_type in (2,4,5,7,9) and trans_status = 'Validate')
                                select distinct t.id,t.policy_id, t.trans_type, t.adl_path,  t.principalval, t.principals_included, stuff((select distinct ',' + cast(ps.id as varchar) for xml path ('')),1,1,'') as policylist, t.storage_url,principal_type,permission_json from trans t inner join policy_snapshot_by_path ps on t.adl_path = replace(replace(ps.adl_path,'[''',''),''']','')   and t.principalval = ps.principal and t.policy_id <> ps.id """

            ### TODO - another variation of this is where a policy match was found at a lower level. Need to honour the current transaction at the higher level recursively but insert a new transaction to apply the permissions at the lower level

            #logging.info(policy_conflicts)
            trans_id = -1
            cursor.execute(policy_conflicts)
            transactions = cursor.fetchall()


            if len(transactions) > 0:
                for transaction_row in transactions:
                    logging.info("Transaction ID = "+str(transaction_row[0]) + " vs transid = "+str(trans_id))
                    if trans_id != transaction_row[0]: #if this is the start of new transaction ID
                        remaining_principals= ''
                        principal_json = json.loads(transaction_row[5])
                        try: 
                            total_users = len(principal_json['userList'])
                        except KeyError:
                            total_users = 0
                        try:
                            total_groups = len(principal_json['groupList'])
                        except KeyError:
                            total_groups = 0
                        total_principals = total_users + total_groups
                        logging.info('total principals '+str(total_principals))

                    if transaction_row[2] in (2,5,7):
                        # find the difference between the current policy change and another policy with the same path for the current principal
                        # TODO try a union of the the inverse of this like pattern to ensure we capture both sub and super sets of these matches
                        policy_delta = """select trim(value) from policy_transactions CROSS APPLY STRING_SPLIT(replace(replace(replace(permission_json,'[',''),']',''),'"',''),',')  where id = """ + str(transaction_row[0]) + """
                                        except 
                                        select ps.permission from policy_snapshot_by_path ps where  '"""+ str(transaction_row[3] + """' = replace(replace(ps.adl_path,'[''',''),''']','') and ps.principal = '""" + str(transaction_row[4]) + """' and ps.id <> """ + str(transaction_row[1]))
                        logging.info(policy_delta)
                        cursorinner.execute(policy_delta)
                        delta_rows = cursorinner.fetchall()
                        if len(delta_rows)==0: # no differences with another policy already applying the same permissions we are about to take away in this change so simply ignore this change as rule of maximum applies
                            logging.info("Rule of maximum: policy "+ str(transaction_row[6]) + " has a permission " + transaction_row[9].lstrip('[').rstrip(']') + " which was requested to be removed, therefore ignore this request...")
                            if total_principals==1: # if there is only one principal in the change / transaction then mark this transaction as ignored because there was found to be a matching policy for this principal with the exact same set of permissions.
                                principal_json[transaction_row[8]].remove(transaction_row[4])
                                remaining_principals = json.dumps(principal_json)

                                logging.info("There was only one principal remaining (now none) in this transaction which will now be ignored due to rule of maximum, therefore the entire transaction is set to ignored...")
                                trans_update = """update policy_transactions set trans_status ='Ignored', principals_included = '"""+ remaining_principals+ """', principals_excluded = trim(',' from concat(principals_excluded,',','"""+ str(transaction_row[4]) + """')), trans_reason=concat(trans_reason,'Policy """ + str(transaction_row[6]) + """ overrides permissions that were to be removed on this path for principal """ + str(transaction_row[4]) +""". This transaction will now be ignored and any remaining changes (if any) will be applied in a sepearate transaction entry.') where id = """ + str(transaction_row[0])
                                logging.info(trans_update)
                                transcursor.execute(trans_update)
                            else: # if not all the princpals were in the transaction then only remove the one in this specific cursor with a match to another policy
                                logging.info("There are multiple principals in this transaction therefore only removing the current principal "+transaction_row[4] + " due to rule of maximum...")
                                principal_json[transaction_row[8]].remove(transaction_row[4])
                                remaining_principals = json.dumps(principal_json)
                                # now recalculate the number of remaining principals and if none are left then mark the entire transaction as ignored
                                try: 
                                    total_users = len(principal_json['userList'])
                                except KeyError:
                                    total_users = 0
                                try:
                                    total_groups = len(principal_json['groupList'])
                                except KeyError:
                                    total_groups = 0
                                total_principals = total_users + total_groups
                                #logging.info(str(total_principals))
                                #logging.info(str(remaining_principals))
                                ## now check whether there are any principals remaining and if not set the entire transaction record to ignored. This might occur if there was more than one principal in the transaction but eventually all of them were removed as they matched to another policy
                                if total_principals == 0:
                                    trans_fix = """update policy_transactions set trans_status ='Ignored', trans_reason=concat(trans_reason,'Policy """ + str(transaction_row[6]) + """ overrides permissions that were to be removed on this path for this/these principals '""" 
                                    + str(transaction_row[4]) +"""'. This transaction will be ignored and any remaining changes will be applied in a sepearate transaction entry (if any).') where id = """ + str(transaction_row[0]) 
                                    logging.info(trans_fix)
                                    #transcursor.execute(trans_fix)
                                else: # there are still remaining prinipals
                                    # remove the principals
                                    ### recalculate means that we need to adjust the ACE entry and principals_included
                                    trans_update = """update policy_transactions set trans_status ='Recalculate', principals_included = '"""+ remaining_principals+ """', principals_excluded = trim(',' from concat(principals_excluded,',','"""+ str(transaction_row[4]) + """')), trans_reason=concat(trans_reason,'....','Policy """ + str(transaction_row[6]) + """ overrides permissions that were to be removed on this path for principal """ + str(transaction_row[4]) +""" therefore this user was excluded.') where id = """ + str(transaction_row[0])
                                    logging.info(trans_update)
                                    transcursor.execute(trans_update)

                        else: 
                            ## TODO - need to apply the matched policy permissions rather than the difference as there is no remove ACLs process. i.e. run a new query here to determine what the permissions should be e.g. no longer have write permissions so just set read
                            permissions = []
                            #orig_perms =  """select permission from policy_snapshot_by_path where id =  """ + str(transaction_row[6]) + """
                            for delta_row in delta_rows:
                              permissions.append(delta_row[0])
                            #permstr = getPermSeq('read') #permissions)
                            #permstr = permstr.ljust(3,'-')
                            permstr = 'r--'
                            logging.info('new permission string ' + permstr)
                            for delta_row in delta_rows:
                                # only partial difference to another policy. if the permission we are trying to remove is = to the difference and if there is only one principal in this transaction then apply it otherwise remove the princpal and add a new transaction entry
                                logging.info("Total principals "+ str(total_principals) + ", delta row "+delta_row[0] + ", req " + transaction_row[9].lstrip('["').rstrip('"]'))
                                if total_principals>1 or delta_row[0] != transaction_row[9].lstrip('["').rstrip('"]'): # if there is more than one principal and the request is different to the another policy then remove this principal 
                                    principal_json[transaction_row[8]].remove(transaction_row[4])
                                    remaining_principals = json.dumps(principal_json)
                                    trans_update = """update policy_transactions set trans_status ='Recalculate', principals_included = '"""+ remaining_principals + """',principals_excluded = trim(',' from concat(principals_excluded,',','"""+ str(transaction_row[4]) + """')), trans_reason=concat(trans_reason,'....','Policy """ + str(transaction_row[6]) + """ overrides some of the permissions that were to be removed on this path for principal """ + str(transaction_row[4]) +""" therefore this user was excluded. A new transaction will be created for the remaining permissions') where id = """ + str(transaction_row[0])
                                    logging.info(trans_update)
                                    transcursor.execute(trans_update)
                                    graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)
                                    spnid =  getSPID(graphtoken,transaction_row[4],'users')
                                    logging.info('user to set '+ spnid)
                                    spids['user'].append(spnid)
                                    acentry = spidsToACEentry(spids,permstr)
                                    captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
                                    transStatus = 'Pending'
                                    transReason = 'Rule of maximum applied to user '+transaction_row[4] + ' as policy '+  str(transaction_row[6]) + ' had partial permissions.'
                                    trans_insert = "insert into " + dbschema + ".policy_transactions (policy_id, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason) " \
                                    " values ('" + str(transaction_row[0]) + "','" + transaction_row[7]  + "','" + transaction_row[3] + "','" + 'setAccessControlRecursive' + "','" + 'modify' + "','" + acentry + "','"+ captureTime + "','7','" + transStatus + "','" + transReason + "')"
                                    logging.info(trans_insert)
                                    transcursor.execute(trans_insert)
                                else:
                                    trans_update = """update policy_transactions set trans_status ='Pending', trans_reason=concat(trans_reason,'Policy """ + str(transaction_row[6]) + """ has different permissions to those being removed on this path for """+ str(transaction_row[4]) + """ therefore proceeding with the transaction requst...') where id = """ + str(transaction_row[0]) 
                                    logging.info(trans_update)
                                    transcursor.execute(trans_update)
                    trans_id = transaction_row[0]
            else: #there are no conflicting policies therefore these transaction should be queued
                trans_update = """update policy_transactions set trans_status ='"""+ new_trans_status + """' where trans_status='Validate'"""
                #logging.info(trans_update)
                logging.info("Updating any other transactions not in conflict awaiting validation...")
                transcursor.execute(trans_update)

    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            logging.info('Done')
    finally:
            cnxn.autocommit = True

def processRecalc():
    tenantid=os.environ["tenantID"]
    spnid= os.environ["SPNID"]
    spnsecret= os.environ["SPNSecret"]

    spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)
    try:
            cursor = cnxn.cursor()
            transcursor = cnxn.cursor()
            policy_recalc = """select id,principals_included,principals_included, adl_permission_str from policy_transactions where trans_status = 'Recalculate'"""
            cursor.execute(policy_recalc)
            transactions = cursor.fetchall()
            for transaction_row in transactions:
              logging.info(json.dumps(transaction_row[2]))
              principals_json = json.loads(transaction_row[2])
              try:
                logging.info(principals_json['userList'])
                for userentry in principals_json['userList']: #will be grouplist next time round
                  spnid = getSPID(graphtoken,userentry,'users')
                  if spnid is not None:
                    spids['user'].append(spnid)
              except KeyError:
                  None
              try:
                logging.info(principals_json['groupList'])
                for userentry in principals_json['groupList']: 
                  spnid = getSPID(graphtoken,userentry,'groups')
                  if spnid is not None:
                    spids['user'].append(spnid)
              except KeyError:
                  None

              acentry = spidsToACEentry(spids,transaction_row[3])   
              logging.info("Ace entry is now : "+acentry)          
              trans_update = """update policy_transactions set trans_status = 'Pending', acentry = '""" + acentry + """' where id = """ + str(transaction_row[0])
              transcursor.execute(trans_update)
    except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error message: '.join(sqlstate))
    else:
            cnxn.commit()
            logging.info('Done')
    finally:
            cnxn.autocommit = True

def storeQueueItems(msg):
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    cursor = cnxn.cursor()
    now =  datetime.datetime.utcnow()
    progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
    # note about the convert statements below, this is merely to convert the time value into the correct format for the fn_cdc_map_time_to_lsn function.
    # either there will be no data in the ctl table (first run) and then all changes are scanned. otherwise there is a last checkpoint found. if this matches the maximum lsn of the database then no changes have happened since the last run ie do nother. otherwise scan for changes...
    sql_txt = "select id,storage_url,trans_action,trans_mode,acentry from " + dbschema + ".policy_transactions where trans_status ='Pending'"
    logging.info(sql_txt)
    cursor.execute(sql_txt)
    row_headers=[x[0] for x in cursor.description] #this will extract row headers
    rows = cursor.fetchall()
    #for row in rows:
    #    logging.info(row)
    json_data=[]
    for result in rows:
        json_data.append(dict(zip(row_headers,result)))
    #logging.info(json.dumps(json_data))
    queue_upd = "update " + dbschema + ".policy_transactions set trans_status = 'Queued' where trans_status ='Pending'"
    try:
        msg.set(json.dumps(json_data))
        cursor.execute(queue_upd)
    except pyodbc.DatabaseError as err:
        cnxn.commit()
        sqlstate = err.args[1]
        sqlstate = sqlstate.split(".")
        logging.error('Error message: '.join(sqlstate))
    except Exception as e:
        logging.error('Error occured when trying to queue ACL work items:' + str(e))
    else:
        cnxn.commit()
        logging.info('Done')
    finally:
        cnxn.autocommit = True

    """for i in json_data:
        msg.set(json.dumps(i))
        logging.info(queue_upd + str(i['id']))
        try:
            cursor.execute(queue_upd + str(i['id']))
        except pyodbc.DatabaseError as err:
            cnxn.commit()
            sqlstate = err.args[1]
            sqlstate = sqlstate.split(".")
            logging.error('Error message: '.join(sqlstate))
        else:
            cnxn.commit()
            logging.info('Done')
        finally:
            cnxn.autocommit = True"""
    #msg.set(list(json_data))

devstage = 'live'
#getPolicyChanges()
#businessRuleValidation()
#processRecalc()
#storeQueueItems() 
## some logic for rule of max
##if removing a user then check whether other perms should still apply and remove just the delta ie if removing a user but has read elsewhere then ignore the user for this specific transaction and capture a transaction to just remove the os.read
##if removing a permission check whether that permission if found in another policy and if so ignore
