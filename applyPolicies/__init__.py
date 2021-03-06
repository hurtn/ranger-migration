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
# 11 - policy resync
# 12 - modification: add tables to inclusion/exclusion
# 13 - modification: remove tables from inclusion/exclusion
# 14 - modification: table type specification was inverted i.e. change from inclusion/exclusion to exclusion/inclusion
# 15 - modification: table level specification set, ie was * now tables have been specified
# 16 - modification: table level specification removed, ie was tables now * have been specified 
# 17 - modification: tables added to table specification
# 18 - modification: tables removed from table specification
# 19 - add database level r-x permission for new policy with table exclusion
# 20 - remove database level r-x permission for deleted policy with table exclusion
# * means these transaction types need to be validated against business rules e.g. rule of maximum

import os
import datetime
import logging
import urllib
import pyodbc
import pandas as pd
import pandas.io.common
import ast
from sqlalchemy import create_engine, true
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
import enum

userExclusionsList = []
groupExclusionsList = []
tenantid=os.environ["tenantID"]
spnid= os.environ.get('SPNID','')
spnsecret= os.environ.get('SPNSecret','')
basestorageuri = os.environ["basestorageendpoint"]
dbname = os.environ["dbname"]
dbschema = os.environ["dbschema"]
principalsIncluded = defaultdict(list)
principalOIDs  = defaultdict(list)
permmappings =  defaultdict(list)

class FilterType(enum.Enum):
    User = 'users'
    Group = 'groups'



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
    processRecalc()
    
   

# Obtains a list of security princpals IDs. Calls getSPID which fetches the ID from AAD
def getSPIDs(userslist, groupslist):
    global exclusionCount
    global allPrincipalsExcluded
    global excludedPrincipals
    global principalsIncluded
    global principalOIDs 
    global tenantid,spnid,spnsecret,basestorageuri,dbschema,dbname
    exclusionCount=0
    excludedPrincipals=[]

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

    graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)
    spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL
    totalValidUsers = 0
    totalValidGroups = 0
    # iterate through the comma separate list of groups and set the dictionary object
    if userslist is not None and len(userslist)>0:
        totalValidUsers = len(userslist)
        #logging.info(str(userslist))
        userentries = str(userslist).split(",")
        for userentry in userentries:
            #logging.info("user: "+userentry.strip("['").strip("']").strip(' '))
            #logging.info('user entry before '+ userentry)
            userentry = userentry.strip("[").strip("]").strip("'").strip(' ').strip("'")
            logging.info('Obtaining user ID: '+userentry)
            if userExclusionsList:
                userentry = applyExclusions(userentry, userExclusionsList)
            if userentry:                
                principalsIncluded['userList'].append(userentry)
                if principalOIDs['u'+userentry]:
                    logging.info('OID dict is not null '+ str(principalOIDs['u'+userentry][0]))
                    oid = principalOIDs['u'+userentry][0]
                else:
                    oid = getSPID(graphtoken,userentry,'users')
                    principalOIDs['u'+userentry].append(oid)

                logging.info('OID is '+str(oid))
                if oid!='':
                    logging.info("Returned spid : "+oid + " for "+ userentry) 
                    spids['user'].append(oid)


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
                if principalOIDs['g'+groupentry]:
                    oid = principalOIDs['g'+groupentry][0]
                else:
                    oid = getSPID(graphtoken,groupentry,'groups')
                    principalOIDs['g'+groupentry].append(oid)

                if oid!='':
                    spids['group'].append(oid)


    logging.info(str(exclusionCount) + '=' + str(totalValidGroups) + ' + ' + str(totalValidUsers))
    if (exclusionCount == totalValidGroups + totalValidUsers):
        logging.info('No remaining principals left as they were all matched to the exclusion list')
        allPrincipalsExcluded = 1
    else:
        allPrincipalsExcluded = 0
    return spids

def captureTransaction(cursor,transaction,transmode, adlpath, spids, pPolicyID, lpermstr, ptranstype, permmap, repo_name):
    #global allPrincipalsExcluded

    transStatus ='Validate' # assume transaction is going to be executed until it fails one of the validation steps
    transReason = '' # valid until proven otherwise
    request_path =''
    http_path = ''
    if (transmode=='modify' and spids and lpermstr) or (transmode=='remove' and spids): # only obtain the ACE entry if the parameters are valid for that specific transmode ie. modify needs both spids and perms and remove only needs spids
        acentry = spidsToACEentry(spids,lpermstr)
    else:
        acentry = ''
    logging.info("Ace entry "+ str(acentry))
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
        logging.error("No storage path obtained for policy "+ str(pPolicyID)+": " + repo_name)
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
        elif adlpath.find("hdfs")>=0:
            # if this is the default hive warehouse location then transform into the ADLS location
            http_path =  adlpath[adlpath.find("/warehouse")+10:]
            storageuri = basestorageuri
        else:
            storageuri = basestorageuri
        request_path = storageuri+http_path

    now =  datetime.datetime.utcnow()
    captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
    transinsert = "insert into " + dbschema + ".policy_transactions (policy_id, repositoryName, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason, all_principals_excluded,principals_excluded,exclusion_list,principals_included, adl_permission_str, permission_json) " \
                    " values ('" + str(pPolicyID) + "','" + repo_name + "','" + request_path  + "','" + adlpath + "','" + transaction + "','" + transmode + "','" + acentry + "','"+ captureTime + "','" + str(ptranstype) + "','" + transStatus + "','" + transReason + "'," +str(allPrincipalsExcluded) + ",'" \
                    "" + ','.join(excludedPrincipals)+"','"+','.join(userExclusionsList)+','.join(groupExclusionsList)+"','" + json.dumps(principalsIncluded) +"','" +  lpermstr + "','" + json.dumps(permmap) + "')"
    logging.info("Capturing transaction: "+transinsert)
    cursor.execute(transinsert)

def syncPolicy(cursor,pPolicyId,pResources,pPermMapList,pTableNames,pTableType,pTables,pRepositoryName):
                    # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
                    hdfsentries = pResources.strip("path=[").strip("[").strip("]").split(",")

                   #Load the json string of permission mappings into a dictionary object
                    permmaplist = json.loads(pPermMapList)
                    tableNames = json.loads(pTableNames)

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

                        if pTableType == 'Exclusion' and pTables != '*': # process at table level
                            logging.info("Table exclusion list in policy detected")
                            tablesToExclude = pTables.split(",")
                            # iterate through the array of tables for this database
                            for tblindb in tableNames:
                                isExcluded = False  # assume not excluded until there is a match
                                for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                    #logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                    if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                        isExcluded = True
                                        logging.info("Table " + tblindb + " is to be excluded from ACLs")
                                if not isExcluded:
                                    logging.info("Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNames[tblindb])  
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', tableNames[tblindb],spids,pPolicyId,permstr,11,permap["permList"], pRepositoryName)

                            # if not a match to tables in the exclusion list then 
                            # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                        else: #capture entry as normal at the database level
                            for hdfsentry in hdfsentries:
                                hdfsentry = hdfsentry.strip().strip("'")
                                logging.info("Capturing transaction for path: " + hdfsentry)
                                captureTransaction(cursor,'setAccessControlRecursive','modify', hdfsentry,spids,pPolicyId,permstr,11,permap["permList"],pRepositoryName)




def getPolicyChanges():

    def aclErrorLogic(aclCount):
        if aclCount == 0:  errorflag = 1 # if no ACLs were ever set then enable the error flag
        if aclCount < 0: errorflag = aclCount  # any other reason may not necessarily be an error but either the stage variable was set to non-live or all users/groups were excluded





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

    def ACLlogic(hdfsentries,rid, rpermList,ruserList,rgroupList,rtableNames,rtables, rtable_type, rrepositoryName,action,trans_type):

                            if action == 'modify':
                              permstr = getPermSeq(rpermList)  
                              permstr = permstr.ljust(3,'-')
                              logging.info("Permissions to be set: " +permstr)
                            else:
                                permstr=''

                            # obtain a list of all security principals
                            spids = getSPIDs(ruserList,rgroupList)
                            logging.warning("!!! tables is " + str(rtables[0]) + " and rtablenames is "+ str(rtableNames))
                            if rtables[0] != '*': # process at table level
                                logging.warning("***** Table exclusion/inclusion list in policy detected: "+str(rtables))
                                # tablesSpecified = rtables.split(",")
                                # iterate through the array of tables for this database
                                for tblindb in rtableNames:
                                    logging.info("Current table in database is "+tblindb)
                                    isExcluded = False  # assume not excluded until there is a match
                                    isIncluded = False
                                    for tblSpecified in rtables: #loop through the tables in the exclusion list
                                        logging.warning("Comparing " +  tblSpecified + " with " + tblindb)
                                        if tblindb == tblSpecified:  # if a match to the exclusion list then set the flag
                                            if rtable_type == 'Exclusion':
                                                isExcluded = True
                                                logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                            if rtable_type == 'Inclusion':
                                                isIncluded = True
                                                logging.warning("***** Table " + tblindb + " is to be included from ACLs")

                                    if (rtable_type == 'Exclusion' and not isExcluded) or (rtable_type == 'Inclusion' and isIncluded):
                                        logging.warning("***** Table " + tblindb + " is to be included, therefore ACLs will be "+ action + " for " + rtableNames[tblindb])  
                                        captureTransaction(cursor,'setAccessControlRecursive',action, rtableNames[tblindb],spids,rid,permstr,trans_type,rpermList, rrepositoryName)
                                if os.environ.get('allowDatabaseLs','0')=='1': # this variable defines whether users should have ls permissions at the database level to see all tables regardless of whether they have access to those tables

                                    # if trans_type = 1 (new policy) or 6 (add principals) or 15 (change to table level) then add read permissions at database level also
                                    if trans_type in (1,15,6):
                                        logging.info("allowDatabaseLs environment variable active, therefore addition r-x permissions will be applied at database level")                                        
                                        for hdfsentry in hdfsentries:
                                            hdfsentry = hdfsentry.strip().strip("'")
                                            captureTransaction(cursor,'setAccessControl','modify', hdfsentry,spids,rid,'r-x',19,rpermList, rrepositoryName)                                      
                                    #if trans_type = 2 (entire policy deleted) then remove read permissions at the database level also
                                    if trans_type in (2,7): # if remove policy or remove principals then remove read permissions at the database level
                                        logging.info("allowDatabaseLs environment variable active, therefore addition r-x permissions will be removed at database level")
                                        for hdfsentry in hdfsentries:
                                            hdfsentry = hdfsentry.strip().strip("'")                                        
                                            captureTransaction(cursor,'setAccessControl','modify', hdfsentry,spids,rid,'---',20,rpermList, rrepositoryName) # using modify with --- (no permissions) which effectively is the same as a remove. using this workaround currently as remove throws an error for setAccessControl (non recursive)
                            elif rtable_type in ('Inclusion') and rtables[0] == '*': #capture entry as normal at the database level
                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                    logging.info(action + " ACLs from policy for path: " + hdfsentry)
                                    captureTransaction(cursor,'setAccessControlRecursive',action, hdfsentry,spids,row.id,permstr,trans_type,rpermList,rrepositoryName)
                            elif rtable_type in ('Exclusion') and rtables[0] == '*': #this is an unusual combination which essentially means do nothing as everything in this database is excluded
                                logging.warn("An unusual configuration was detected. All tables * were excluded. Please check with the policy administrator that this was the desired configuration.")
                            else:
                                logging.error("Could not determine policy configuration. Please contact your administrator.")



    connxstr=os.environ["DatabaseConnxStr"]

    global dbschema
    global basestorageuri
    global userExclusionsList
    global groupExclusionsList
    global permmappings

    errorflag=0
    #allPrincipalsExcluded = 0
    exclusionCount=0
    #excludedPrincipals=[]
    cnxn = pyodbc.connect(connxstr)

    try:
            userExclusionsList =[]
            groupExclusionsList =[]
            # configure database params
            # connxstr=os.environ["DatabaseConnxStr"]
            appname = 'applyPolicies'
            targettablenm = "ranger_policies"
            batchsize = 200
            #params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            #collist = ['ID','Name','Resources','Groups','Users','Accesses','Service Type','Status']
            #ID,Name,Resources,Groups,Users,Accesses,Service Type,Status

            #cnxn = pyodbc.connect(connxstr)
            cursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')

            # fetch exclusion list
            sql_txt = "select * from " + dbschema + ".exclusions where type in ('G','U');"
            cursor.execute(sql_txt)
            row = cursor.fetchone()
            while row:
                if row[1] == 'U':
                  userExclusionsList.append(str(row[2]))
                if row[1] == 'G':
                  groupExclusionsList.append(str(row[2]))  
                row = cursor.fetchone()

            # fetch permission mappings
            cursor.execute("select ranger_perm,adls_perm from perm_mapping  order by case when adls_perm ='r' then 1 when adls_perm = 'w' then 2 else 3 end") #correctly ordered for adls permission form rwx
            rows = cursor.fetchall()
            for row in rows:
                permmappings[row[0]].append(row[1])


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
            select [__$operation],[id],[Name],coalesce(resources,paths) Resources,[Status],replace(permMapList,'''','"') permMapList,[Service Type],tables,table_type,table_names, repositoryName
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

            #if not (insertdf.empty and updatesdf.empty and deleteddf.empty): # if there are either inserts or updates then only get tokens
                #storagetoken = getBearerToken(tenantid,"storage.azure.com",spnid,spnsecret)
                #graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)

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
                for row in insertdf.loc[:, ['__$operation','id','Name','Resources','Status','permMapList','Service Type','table_type','tables','table_names','repositoryName']].itertuples():
                    
                    if row.Status in ('Enabled','True') : 
 
                        for permap in json.loads(row.permMapList): #this loop iterates through each permMapList and applies the ACLs

                            ACLlogic(row.Resources.strip("path=[").strip("[").strip("]").split(","),row.id,permap["permList"],permap["userList"],permap["groupList"],json.loads(row.table_names),row.tables.split(","),row.table_type, row.repositoryName,'modify',1)
                        """
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

                            if row.tables != '*': # process at table level
                              logging.warning("***** Table exclusion list in policy detected")
                              tablesSpecified = row.tables.split(",")
                              # iterate through the array of tables for this database
                              for tblindb in tableNames:
                                  isExcluded = False  # assume not excluded until there is a match
                                  isIncluded = False
                                  for tblSpecified in tablesSpecified: #loop through the tables in the exclusion list
                                      logging.warning("Comparing " +  tblSpecified + " with " + tblindb)
                                      if tblindb == tblSpecified:  # if a match to the exclusion list then set the flag
                                          if row.table_type == 'Exclusion':
                                              isExcluded = True
                                              logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                          if row.table_type == 'Inclusion':
                                              isIncluded = True
                                              logging.warning("***** Table " + tblindb + " is to be included from ACLs")

                                  if (row.table_type == 'Exclusion' and not isExcluded) or (row.table_type == 'Inclusion' and isIncluded):
                                    logging.warning("***** Table " + tblindb + " is to be included, therefore ACLs will be added to " + tableNames[tblindb])  
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', tableNames[tblindb],spids,row.id,permstr,1,permap["permList"], row.repositoryName)

                            elif row.table_type in ('Inclusion') and row.tables == '*': #capture entry as normal at the database level
                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    logging.info("Passing path: " + hdfsentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','modify', hdfsentry,spids,row.id,permstr,1,permap["permList"],row.repositoryName)
                            elif row.table_type in ('Exclusion') and row.tables == '*': #this is an unusual combination which essentially means do nothing as everything in this database is excluded
                                logging.warn("An unusual configuration was detected. All tables * were excluded. Please check with the policy administrator that this was the desired configuration.")
                            else:
                                logging.error("Could not determine policy configuration. Please contact your administrator.")
                        """
            if not deleteddf.empty:
            #################################################
            #                                               #
            #               Deleted Policies                #
            #                                               #
            ################################################# 


                logging.info("\nDeleted policy rows detected:")
                logging.info(deleteddf)
                logging.info("\n")

                # iterate through the deleted policy rows
                for row in deleteddf.loc[:, ['__$operation','id','Name','Resources','Status','permMapList','Service Type','table_type','tables','table_names','repositoryName']].itertuples():
                    
                    if row.Status in ('Enabled','True'): # only bother deleting ACLs where the policy was in an enabled state
                        """
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

                            if row.tables != '*': # process at table level
                              logging.warning("***** Table exclusion list in policy detected")
                              tablesSpecified = row.tables.split(",")
                              # iterate through the array of tables for this database
                              for tblindb in tableNames:
                                  isExcluded = False  # assume not excluded until there is a match
                                  isIncluded = False
                                  for tblSpecified in tablesSpecified: #loop through the tables in the exclusion list
                                      logging.warning("Comparing " +  tblSpecified + " with " + tblindb)
                                      if tblindb == tblSpecified:  # if a match to the exclusion list then set the flag
                                          if row.table_type == 'Exclusion':
                                              isExcluded = True
                                              logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                          if row.table_type == 'Inclusion':
                                              isIncluded = True
                                              logging.warning("***** Table " + tblindb + " is to be included from ACLs")

                                  if (row.table_type == 'Exclusion' and not isExcluded) or (row.table_type == 'Inclusion' and isIncluded):
                                    logging.warning("***** Table " + tblindb + " is to be included, therefore ACLs will be removed from " + tableNames[tblindb])  
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', tableNames[tblindb],spids,row.id,'',1,'', row.repositoryName)

                            elif row.table_type in ('Inclusion') and row.tables == '*': #capture entry as normal at the database level
                                for hdfsentry in hdfsentries:
                                    hdfsentry = hdfsentry.strip().strip("'")
                                    # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                    logging.info("Removing ACLs from deleted policy for path: " + hdfsentry)
                                    captureTransaction(cursor,'setAccessControlRecursive','remove', hdfsentry,spids,row.id,'',2,'',row.repositoryName)
                            elif row.table_type in ('Exclusion') and row.tables == '*': #this is an unusual combination which essentially means do nothing as everything in this database is excluded
                                logging.warn("An unusual configuration was detected. All tables * were excluded. Please check with the policy administrator that this was the desired configuration.")
                            else:
                                logging.error("Could not determine policy configuration. Please contact your administrator.")
                        """
                        for permap in json.loads(row.permMapList): #this loop iterates through each permMapList and applies the ACLs

                            ACLlogic(row.Resources.strip("path=[").strip("[").strip("]").split(","),row.id,permap["permList"],permap["userList"],permap["groupList"],json.loads(row.table_names),row.tables.split(","),row.table_type, row.repositoryName,'remove',2)



            #logging.info("Determining any other changes...")

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
                tableTypeChanged = False
                #aclsForAllPathsSet = False #this variable is an modified policy optimisation - for example if the changes included a new permisssion and simultaneously a new path/database was added there is no need to do these both as there would be duplication so we set a flag when one is done to avoid doing the other

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
                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]] is not None:
                        resourcesafter = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]])
                    else:
                        resourcesafter =  ''

                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]] is not None:
                        resourcesbefore = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]])
                    else:
                        resourcesbefore = resourcesafter # note this is done because of the way CDC provides changes for operation type 3 (updates) for columns with varchar(max) i.e. if the before image is NULL this means no change occured, therefore we set it to the current/after value
                                                         # see https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver15#remarks

                    if firstandlastforid.get(key = 'table_names')[firstandlastforid.tail(1).index[0]] is not None:
                        tableNamesAfter = json.loads(firstandlastforid.get(key = 'table_names')[firstandlastforid.tail(1).index[0]])
                    else:
                        tableNamesAfter = ''

                    if  firstandlastforid.get(key = 'table_names')[firstandlastforid.head(1).index[0]] is not None:
                        tableNamesBefore = json.loads(firstandlastforid.get(key = 'table_names')[firstandlastforid.head(1).index[0]])
                    else:
                        tableNamesBefore = tableNamesAfter

                    
                elif row['Service Type']=='hdfs':
                    #resourcesbefore = firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]].split(",")
                    #resourcesafter = firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]].split(",")   # note the syntax [firstandlastforid.tail(1).index[0]] fetches the index of the last record in case there were multiple changes
                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]] is not None:
                        resourcesafter = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.tail(1).index[0]])
                    else:
                        resourcesafter =  ''

                    if firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]] is not None:
                        resourcesbefore = ast.literal_eval(firstandlastforid.get(key = 'Resources')[firstandlastforid.head(1).index[0]])
                    else:
                        resourcesbefore = resourcesafter # note this is done because of the way CDC provides changes for operation type 3 (updates) for columns with varchar(max) i.e. if the before image is NULL this means no change occured, therefore we set it to the current/after value
                                                         # see https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver15#remarks

                else:
                    resourcesbefore = ''
                    resourcesafter = ''

                logging.info("Resources before: "+ str(resourcesbefore))
                logging.info("Resources after: " + str(resourcesafter))

                statusbefore = firstandlastforid.get(key = 'Status')[firstandlastforid.head(1).index[0]].strip()
                statusafter = firstandlastforid.get(key = 'Status')[firstandlastforid.tail(1).index[0]].strip()

                # load the permMapList into a json aray
                if firstandlastforid.get(key = 'permMapList')[firstandlastforid.tail(1).index[0]] is not None:
                    permMapAfter = json.loads(firstandlastforid.get(key = 'permMapList')[firstandlastforid.tail(1).index[0]])
                else:
                    permMapAfter =''

                if firstandlastforid.get(key = 'permMapList')[firstandlastforid.head(1).index[0]] is not None:
                    permMapBefore = json.loads(firstandlastforid.get(key = 'permMapList')[firstandlastforid.head(1).index[0]])
                else:
                    permMapBefore = permMapAfter # note this is done because of the way CDC provides changes for operation type 3 (updates) for columns with varchar(max) i.e. if the before image is NULL this means no change occured, therefore we set it to the current/after value
                                                         # see https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver15#remarks


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


                if firstandlastforid.get(key = 'tables')[firstandlastforid.tail(1).index[0]] is not None:
                    tableListAfter = firstandlastforid.get(key = 'tables')[firstandlastforid.tail(1).index[0]].strip("[").strip("]").split(",")
                else:
                    tableListAfter=''  # note this is done because of the way CDC provides changes for operation type 3 (updates) for columns with varchar(max) i.e. if the before image is NULL this means no change occured, therefore we set it to the current/after value
                                                         # see https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver15#remarks

                if firstandlastforid.get(key = 'tables')[firstandlastforid.head(1).index[0]] is not None:
                    tableListBefore =  firstandlastforid.get(key = 'tables')[firstandlastforid.head(1).index[0]].strip("[").strip("]").split(",")

                else:
                    tableListBefore=tableListAfter

                logging.info('Table list before '+ str(tableListBefore))
                logging.info('Table list after '+ str(tableListAfter))

                if tableTypeBefore != tableTypeAfter: #and tableTypeAfter == 'Exclusion': #and tableListAfter[0] != "*": 
                    tableTypeChanged = True
                    if tableTypeAfter == 'Exclusion':
                        logging.info('Table type changed from Inclusion to Exclusion!')
                        tableExclusionSet = True
                    else:
                        logging.info('Table type changed from Exclusion to Inclusion!')
                        tableExclusionRemoved = True

                else: 
                    tableExclusionSet=False
                    tableTypeChanged = False


                # iterate through the permaplist array
                # maplistmax sets the upper bound of the number of elements in this array because there can be multiple allow conditions set in ranger
                # so we need to loop through each of them and process the changes independently 
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
                    addaccesses = entitiesToAdd(accessesbefore,accessesafter)
                    removeaccesses = entitiesToRemove(accessesbefore,accessesafter) 

                    # determine if any of the table specifications changed
                    addtables = entitiesToAdd(tableListBefore,tableListAfter)
                    removetables = entitiesToRemove(tableListBefore,tableListAfter)    
                    logging.info('Tables to add: '+str(addtables))
                    logging.info('Tables to remove: '+str(removetables))
                    #check if they are really different even if the order was simply changed
                    if addtables or removetables:

                        if check_if_equal(addtables, removetables):
                            logging.info('Tables in before and after lists are equal i.e. contain similar elements with same frequency, negating any changes required')
                            addtables = None
                            removetables = None


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
                                captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,3,accessesafter,row.repositoryName)


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
                                captureTransaction(cursor,'setAccessControlRecursive','remove', resourceentry,spids,row.id,'',4,accessesafter,row.repositoryName)


                    elif statusafter in ('Enabled','True'): # if there wasn't a status change, then only bother dealing with modifications is the policy is set to enabled i.e. we don't care about policies that were disabled prior to the change and are still not enabled. when / if they are eventually enabled they will be treated as any new policy would

                        # check for new or modified hive objects which will result in a change in paths to add ACLs to. 
                        if resourcesbefore != '':
                            addresources = entitiesToAdd(resourcesbefore,resourcesafter)
                            removeresources = entitiesToRemove(resourcesbefore,resourcesafter)   
                        #

                        # process incremental changes to groups, users, accesses or resources if there wasn't a status change above which would take care of these for a specific policy
                        if addgroups or addusers or removegroups or removeusers or removeaccesses or addaccesses or tableTypeChanged or addresources or removeresources or addtables or removetables: 

                            if removeresources:

                                #spids = getSPIDs(usersbefore,groupsbefore)

                                #for resourcetoremove in removeresources:
                                #    resourcetoremove = resourcetoremove.strip().strip("'")
                                #    logging.info("Removing ACLs from deleted directory path: " + resourcetoremove)
                                #    captureTransaction(cursor,'setAccessControlRecursive','remove', resourcetoremove,spids,row.id,'',9,accessesbefore,row.repositoryName)
                                ACLlogic(removeresources,row.id,accessesbefore,usersafter,groupsafter,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'remove',9)


                            if addresources:
                                logging.info("add the new permissions to the following resources")
                                #spids = getSPIDs(usersafter,groupsafter)
                                #for resourcetoadd in addresources:
                                #    resourcetoadd = resourcetoadd.strip().strip("'")
                                #    logging.info("Adding ACLs to new directory path: " + resourcetoadd)
                                #    captureTransaction(cursor,'setAccessControlRecursive','modify', resourcetoadd,spids,row.id,permstr,10,accessesafter,row.repositoryName)

                                ACLlogic(addresources,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'modify',10)



                            # if table type changed from inclusion to exclusion or exclusion to inclusion
                            #  
                            # Remove all previously applied permissions for database/tables using the before image
                            # and now only apply permissions for the database/tables in the after image)   

                            if tableTypeChanged: 
                                logging.info("Processing table type change: " + str(tableNamesBefore) + " to " + str(tableListBefore))
                                ACLlogic(resourcesbefore,row.id,'',usersbefore,groupsbefore,tableNamesBefore,tableListBefore,tableTypeBefore,row.repositoryName,'remove',14)
                                ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'modify',14)

                            if addtables or removetables:
                                # these first few conditions handle the case where * was used
                                # note we deal with table type inversions (i.e. exclusion to inclusion or vise versa) in the section above
                                if tableListBefore[0]=='*' and tableListAfter[0]!='*' and  tableTypeBefore=='Inclusion' and tableTypeAfter=='Inclusion': # in this case remove all permissions except for the tables left in the inclusion list
                                    logging.info('Inclusion list changed from * to table specific list: '+str(addtables))
                                    ACLlogic(resourcesbefore,row.id,'',usersbefore,groupsbefore,tableNamesAfter,tableListAfter,'Exclusion',row.repositoryName,'remove',15)
                                elif tableListBefore[0]!='*' and tableListAfter[0]=='*' and  tableTypeBefore=='Inclusion' and tableTypeAfter=='Inclusion': # in this case we trick/optimise to only add permissions to all the other tables that weren't in the inclusion list before. This is done by a fake exclusion on those tables
                                    logging.info('Inclusion list changed from table specific list to *. Add permissions for all the other tables other than : '+str(tableListBefore))
                                    ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListBefore,'Exclusion',row.repositoryName,'modify',16)                                    
                                elif tableListBefore[0]=='*' and tableListAfter[0]!='*' and  tableTypeBefore=='Exclusion' and tableTypeAfter=='Exclusion': # this is a very unusual / odd case, essentially treat as if it's a new policy with table level exclusions
                                    logging.info('Exclusion list changed from table specific list to *. Add permissions for all the other tables other than : '+str(tableListBefore))
                                    ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListAfter,'Exclusion',row.repositoryName,'modify',15)                                    
                                elif addtables:
                                    acltabletype = 'Inclusion' # set permissions on the tables in the addtable list only
                                    if tableTypeBefore == 'Exclusion' and tableTypeAfter == 'Exclusion': # if a new table was added to the exclusions list then remove the ACLs from that table
                                        aclaction = 'remove'
                                        logging.info("Based on type " + tableTypeAfter + " the acl action is set to  "+ aclaction + " from following new tables were added to the table specification: "+str(addtables))                                   
                                        ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,addtables,acltabletype,row.repositoryName,aclaction,17)

                                    elif  tableTypeBefore == 'Inclusion' and tableTypeAfter == 'Inclusion': # if a table was added to the inclusion list then add permission for that table 
                                        aclaction = 'modify'
                                        logging.info("Based on type " + tableTypeAfter + " the acl action is set to  "+ aclaction + " from following new tables were added to the table specification: "+str(addtables))                                   
                                        ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,addtables,acltabletype,row.repositoryName,aclaction,17)
                                    else:
                                        None #effectively ignore this as it will be taken care of by tableTypeChanged clause above which completely reapplies the permissions for the database
                                elif removetables:
                                    acltabletype = 'Inclusion' # remove permissions on the tables in the remove list only                                    
                                    logging.info("The following tables were removed from the table specification: "+str(removetables))
                                    if tableTypeBefore == 'Exclusion' and tableTypeAfter == 'Exclusion': # if a new table was removed from the exclusions list then add permissions for that table
                                        aclaction = 'modify'
                                        logging.info("Based on type " + tableTypeAfter + " the acl action is set to  "+ aclaction + " the following tables which were removed from the table specification: "+str(addtables))                                       
                                        ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,removetables,acltabletype,row.repositoryName,aclaction,18)

                                    elif tableTypeBefore == 'Inclusion' and tableTypeAfter == 'Inclusion': # if a table was added to the inclusion list then add permission for that table 
                                        aclaction = 'remove'
                                        logging.info("Based on type " + tableTypeAfter + " the acl action is set to  "+ aclaction + " the following tables which were removed from the table specification: "+str(addtables))                                       
                                        ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,removetables,acltabletype,row.repositoryName,aclaction,18)
                                    else:
                                        None #effectively ignore this as it will be taken care of by tableTypeChanged clause above which completely reapplies the permissions for the database



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

                                ACLlogic(resourcesbefore,row.id,'',removeusers,removegroups,tableNamesBefore,tableListBefore,tableTypeBefore,row.repositoryName,'remove',5)
                                """spids = getSPIDs(removeusers,removegroups)

                                if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                                    logging.warning("***** Table exclusion list in policy detected")
                                    tablesToExclude = row.tables.split(",")
                                    # iterate through the array of tables for this database
                                    for tblindb in tableNamesAfter:
                                        isExcluded = False  # assume not excluded until there is a match
                                        for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                            logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                            if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                                isExcluded = True
                                                logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                        if not isExcluded:
                                            logging.warning("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNamesAfter[tblindb])  
                                            captureTransaction(cursor,'setAccessControlRecursive','remove', tableNamesAfter[tblindb],spids,row.id,'',5,accessesafter, row.repositoryName)

                                # if not a match to tables in the exclusion list then 
                                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
                                else: #capture entry as normal at the database level
                                    for resourceentry in resourcesbefore:
                                        resourceentry = resourceentry.strip().strip("'")
                                        # obtain a list of all security principals, ignore exclusions and where rule of maximum applies
                                        logging.info("Capturing ACL transaction for path: " + resourceentry)
                                        captureTransaction(cursor,'setAccessControlRecursive','remove', resourceentry,spids,row.id,'',5,accessesafter,row.repositoryName)                                    
                                """
                            if addgroups or addusers: 
                                if addgroups:
                                    logging.info("Add the following groups: ")
                                    for grouptoadd in addgroups:
                                        logging.info(grouptoadd)
                                if addusers:    
                                    logging.info("Add the following users")
                                    for usertoadd in addusers:
                                        logging.info(usertoadd)
                                ACLlogic(resourcesbefore,row.id,accessesafter,addusers,addgroups,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'modify',6)
                                """
                                spids = getSPIDs(addusers,addgroups)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference

                                if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                                    logging.warning("***** Table exclusion list in policy detected")
                                    tablesToExclude = row.tables.split(",")
                                    # iterate through the array of tables for this database
                                    for tblindb in tableNamesAfter:
                                        isExcluded = False  # assume not excluded until there is a match
                                        for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                            logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                            if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                                isExcluded = True
                                                logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                        if not isExcluded:
                                            logging.warning("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNamesAfter[tblindb])  
                                            captureTransaction(cursor,'setAccessControlRecursive','modify', tableNamesAfter[tblindb],spids,row.id,permstr,6,accessesafter, row.repositoryName)

                                # if not a match to tables in the exclusion list then 
                                else: #capture entry as normal at the database level
                         
                                    for resourceentry in resourcesafter:
                                        resourceentry = resourceentry.strip().strip("'")
                                        #logging.info('Were all principals excluded? ' + str(allPrincipalsExcluded))
                                        logging.info("Passing path: " + resourceentry)
                                        captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,6,accessesafter,row.repositoryName)
                                """
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

                                ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'modify',7)
                                """
                                spids = getSPIDs(usersafter,groupsafter)

                                if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                                    logging.warning("***** Table exclusion list in policy detected")
                                    tablesToExclude = row.tables.split(",")
                                    # iterate through the array of tables for this database
                                    for tblindb in tableNamesAfter:
                                        isExcluded = False  # assume not excluded until there is a match
                                        for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                            logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                            if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                                isExcluded = True
                                                logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                        if not isExcluded:
                                            logging.warning("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNamesAfter[tblindb])  
                                            captureTransaction(cursor,'setAccessControlRecursive','modify', tableNamesAfter[tblindb],spids,row.id,permstr,7,removeaccesses, row.repositoryName)
                                # if not a match to tables in the exclusion list then 
                                else: #capture entry as normal at the database level

                                    for resourceentry in resourcesafter:
                                        resourceentry = resourceentry.strip().strip("'")
                                        logging.info("Passing path: " + resourceentry)
                                        captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,7,removeaccesses,row.repositoryName)
                                """
                            if addaccesses:
                                logging.info("add the following accesses")
                                for accesstoadd in addaccesses:
                                    logging.info(accesstoadd)
                                ACLlogic(resourcesafter,row.id,accessesafter,usersafter,groupsafter,tableNamesAfter,tableListAfter,tableTypeAfter,row.repositoryName,'modify',8)

                                """
                                spids = getSPIDs(usersafter,groupsafter)  ## Note: here we could potentially use the rowafter.Users/Groups list (i.e. the current image of groups) instead of the delta/difference

                                if row.table_type == 'Exclusion' and row.tables != '*': # process at table level
                                    logging.warning("***** Table exclusion list in policy detected")
                                    tablesToExclude = row.tables.split(",")
                                    # iterate through the array of tables for this database
                                    for tblindb in tableNamesAfter:
                                        isExcluded = False  # assume not excluded until there is a match
                                        for tblToExclude in tablesToExclude: #loop through the tables in the exclusion list
                                            logging.warning("Comparing " +  tblToExclude + " with " + tblindb)
                                            if tblindb == tblToExclude:  # if a match to the exclusion list then set the flag
                                                isExcluded = True
                                                logging.warning("***** Table " + tblindb + " is to be excluded from ACLs")
                                        if not isExcluded:
                                            logging.warning("***** Table " + tblindb + " was not found on the table exclusion list, therefore ACLs will be added to " + tableNamesAfter[tblindb])  
                                            captureTransaction(cursor,'setAccessControlRecursive','modify', tableNamesAfter[tblindb],spids,row.id,permstr,8,addaccesses, row.repositoryName)
                                # if not a match to tables in the exclusion list then 
                                else: #capture entry as normal at the database level

                                    for resourceentry in resourcesafter:
                                        resourceentry = resourceentry.strip().strip("'")
                                        logging.info("Passing path: " + resourceentry)
                                        captureTransaction(cursor,'setAccessControlRecursive','modify', resourceentry,spids,row.id,permstr,8,addaccesses,row.repositoryName)
                                """
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
        
            params = urllib.parse.quote_plus(connxstr)

            conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
            engine = create_engine(conn_str,echo=False)

            # sql alchemy listener
            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(
            conn, cursor, statement, params, context, executemany
                ):
                    if executemany:
                        cursor.fast_executemany = True

            # loading aad cache
            tempdf = pd.DataFrame()
            tempdf = pd.DataFrame(list(principalOIDs.items()),columns = ['AAD_principal_name','AAD_OID'])

            logging.info("applyPolicies: Saving a copy of AAD principals into cache table")
            #tempdf = tempdf.set_index('AAD_principal_name')
            aadcachedf = pd.concat([tempdf.drop(columns='AAD_OID'),pd.DataFrame(tempdf['AAD_OID'].tolist(), columns=['AAD_OID'])],axis=1)
            aadcachedf.to_sql("aad_cache_staging",engine,index=False,if_exists="replace")

            rowcount = -1
            mergesql = """MERGE """ + dbname + """.""" + dbschema + """.aad_cache AS Target
            USING (select aad_principal_name, aad_oid from  """ + dbname + """.""" + dbschema + """.aad_cache_staging) AS Source
            ON (Target.[aad_principal_name] = Source.[aad_principal_name])
            WHEN MATCHED THEN
                UPDATE SET Target.[aad_oid] = Source.[aad_oid]
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (aad_principal_name,aad_oid)
                VALUES (
                Source.[aad_principal_name]
                , Source.[aad_oid]); """
            #logging.info(mergesql)
            rowcount = cursor.execute(mergesql).rowcount
            cnxn.commit()
            logging.info(str(rowcount) + " rows merged into aad cache table")

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
            logging.info("Warning: Could not find user ID of "+spname+". Response: "+str(response))
            if spname.strip().replace('#','%23') == 'nick.hurt': 
                logging.info("Returning hard coded user OID")
                return 'cb0c78ea-0032-411a-ae61-0c616d2560e8'
            if spname.strip().replace('#','%23') == 'aramanath': 
                logging.info("Returning hard coded user OID")
                return '818c16bc-2ab3-41bd-bd7f-ea0124b931f0'

            # at this point should we aboort the process or just log the failure?? TBD
            return None
    elif r.status_code>=400 and r.status_code<500:
        if spname.strip().replace('#','%23') == 'nick.hurt': return 'cb0c78ea-0032-411a-ae61-0c616d2560e8'
        if spname.strip().replace('#','%23') == 'aramanath': return '818c16bc-2ab3-41bd-bd7f-ea0124b931f0'
        else:
          logging.warning("Warning: Could not find user ID!!! Response: "+str(r.status_code) + ": "+r.text)
    else:
        logging.warning("Warning: Could not find user ID!!! Response: "+str(r.status_code) + ": "+r.text)
        # at this point should we aboort the process or just log the failure?? TBD by client
        return None


def spidsToACEentry(spids,permissions):
    aceentry = ""
    if spids:
        for sp in spids:
            logging.info('SPID' + str(spids[sp]))
            for spid in spids[sp]:
                logging.info("Preparing " + sp + ' permissions for ' + spid)
                if permissions:
                    aceentry += sp+':'+spid+ ':'+permissions+',default:'+sp+':'+spid + ':'+permissions +','
                else: # the specification to remove ACLs doesn't require a perm str, only the SPID(s)
                    aceentry += sp+':'+spid +',default:'+sp+':'+spid +','

        aceentry = aceentry.rstrip(',') 
        return aceentry
    else:
        return None

def getPermSeq(perms):
    global permmappings

    #if permmappings is None:
        #logging.warning('Using system default permission mappings. If necessary customise permission mappings via the perm_mapping table')
    cpermstr = ''
    lpermstr = ''
    readperm ='-'
    writeperm = '-'
    execperm = '-'
    for perm in perms:
        for i in permmappings[perm]:
            if i =='r': readperm = i
            if i =='w': writeperm = i
            if i =='x': execperm = i

            #if i == 'w' and cpermstr.find('r')<0  and cpermstr.find('w')<0: cpermstr += '-' # pad the permission string with - if no read permission exists
            #if i == 'x' and lpermstr.find('rw')<0: lpermstr += '--' # pad the permission string with - if no read permission exists
            #if (i == 'r' and cpermstr.find('r')<0) or (i == 'w' and cpermstr.find('w')<0) or ((i == 'x' and cpermstr.find('x')<0)): # only add the permission if it doesn't exist
                #cpermstr += i
    if cpermstr.find('x')<0 and os.environ.get('addeXecute','0')=='1':  #this environment variable can be set to always ensure execute is added to the permission str
        logging.info("addeXecute environment variable active therefore always add execute permission if it does not exist")
        cpermstr = readperm + writeperm + 'x' # always add the execute for directory traversal
    else:
        cpermstr = readperm + writeperm + execperm

    logging.info('custom permstr to return='+cpermstr+'.')

    lpermstr=''
    for perm in perms:
        logging.info('perm to lookup='+perm+'.')
        if perm.strip()  == 'read' and lpermstr.find('r')<0 : lpermstr='r'
        elif perm.strip() == 'select' and lpermstr.find('r')<0: lpermstr='r'
        elif perm.strip() == 'write' and lpermstr.find('r')<0 and lpermstr.find('w')<0: lpermstr='-wx' # this is a special case where no read permissions were found
        elif perm.strip() == 'write' and lpermstr.find('r')>=0 and lpermstr.find('w')<0: lpermstr='rwx'
        elif perm.strip() == 'update' and lpermstr.find('r')<0 and lpermstr.find('w')<0: lpermstr='-wx'
        elif perm.strip() == 'update' and lpermstr.find('r')>=0 and lpermstr.find('w')<0: lpermstr='rwx'
        elif perm.strip() == 'execute' and lpermstr.find('rw')<0 and lpermstr.find('x')<0: lpermstr+='--x'
        elif perm.strip() == 'execute' and lpermstr.find('x')<0: lpermstr+='x'
        elif perm.strip() == 'all': return 'rwx'
        else: lpermstr+=''
    
    if cpermstr!=lpermstr:
        logging.warn('Custom permstr is not the same as default permstr. cpermstr = '+cpermstr + '.lpermstr='+lpermstr)
    logging.info('default permstr to return='+lpermstr+'.')
    return cpermstr
#    else:
 

def getBearerToken(tenantid,resourcetype,spnid,spnsecret):
    bearertoken = ''
    endpoint = 'https://login.microsoftonline.com/' + tenantid 
    #if spnid!='' and spnsecret!='':
    endpoint = endpoint + '/oauth2/token'
    #else:
    #     endpoint = endpoint + '/MSI/token'  #if no service principal details then use managed identity
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    # if service principal auth then use client credentials flow
    if spnid!='' and spnsecret!='':
        endpoint = 'https://login.microsoftonline.com/' + tenantid 
        endpoint = endpoint + '/oauth2/token'

        payload = 'grant_type=client_credentials&client_id='+spnid+'&client_secret='+ spnsecret + '&'
        payload = payload +'resource=https%3A%2F%2F'+resourcetype+'%2F'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(endpoint, headers=headers, data=payload)
    else: #managed identity auth flow requires now client creds so just initialise this variable
        endpoint = os.environ["IDENTITY_ENDPOINT"]
        endpoint = endpoint +'?resource=https://'+resourcetype+'/&api-version=2019-08-01'
        identity_header_val = os.environ["IDENTITY_HEADER"]
        headers = {'X-IDENTITY-HEADER': identity_header_val}
        #payload = 'resource=https%3A%2F%2F'+resourcetype+'&api-version=2019-08-01'
        payload = ''
        r = requests.get(endpoint, headers=headers)


    logging.info('AAD request to endpoint ='+ endpoint + ' with payload = '+ payload )
    logging.info(str(r))
    response = r.json()
    logging.info("Obtaining AAD bearer token for resource "+ resourcetype + "...")
    try:
      bearertoken = response["access_token"]
    except KeyError:
      logging.error("Error coud not obtain bearer token, check identity has necessary permissions: "+ str(response))
    #logging.info(bearertoken)
    logging.info("Bearer token obtained.\n")
    return bearertoken

def businessRuleValidation():
    def policyCorrection(pCursor, pStatusFromStr, pStatusToStr,pReSyncBool):
            trans_to_abort = """with trans as (
                        select id,policy_id, trans_type, adl_path, principalexp.value principalval, principals_included,storage_url, 'userList' principal_type, permission_json, trans_status, repositoryName from policy_transactions pt cross apply OPENJSON(principals_included) with (users nvarchar(max) '$.userList' as json) cross APPLY OPENJSON(users) principalexp)
                        select o.id,o.trans_status, t.id, o.policy_id,o.repositoryName, t.adl_path,o.adl_path,t.principalval, o.principalval from trans t inner join trans o on t.policy_id = o.policy_id and t.trans_status = 'Validate' and o.trans_status in ('Pending','Queued','De-queued','InProgress') and (t.trans_type in (""" + pStatusToStr + """) and o.trans_type ="""+ pStatusFromStr + """) """

            cursor.execute(trans_to_abort)
            transactions = cursor.fetchall()
            logging.info("Determine whether any transactions in flight from " + pStatusFromStr + " to status " + pStatusToStr + "...")
            if len(transactions) > 0:
                logging.info("Policy corrections detected...")
                for transaction_row in transactions:

                  trans_abort  = """update policy_transactions set trans_status = 'Abort', trans_reason= concat(trans_reason,'Transaction aborted as user error correction detected in transaction """+ str(transaction_row[2]) + """') where id = """ + str(transaction_row[0])
                  logging.info(trans_abort)
                  cursor.execute(trans_abort)
                  cnxn.commit() 
                  if pReSyncBool and pStatusToStr in (5,9) and ((transaction_row[5] != transaction_row[6]) or (transaction_row[7] != transaction_row[8])): # only resync if there was a partial correction
                     reSyncPolicy(transaction_row[3],transaction_row[4])

                
    tenantid=os.environ["tenantID"]
    spnid=os.environ["SPNID"]
    spnsecret= os.environ["SPNSecret"]
    spids = defaultdict(list) # a dictionary object of all the security principal (sp) IDs to be set in this ACL
    lPrincipalsIncluded = defaultdict(list) # a dictionary object to store the security principal (sp) being removed from the transaction in conflict and added to a new transaction entry
    def entitiesToRemove(beforelist, afterlist):
        return (list(set(beforelist) - set(afterlist)))                    

    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)

    try:
            ##########################################
            # User error correction detection 
            # 
            # Based on the following transaction states
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
            # 11 - policy resync
            # 
            # - supports the following scenarios where one transaction is inflight (State=InProgress)
            #            
            # Scenario1: Policy deleted or disabled immediately after creation (complete correction or undecided)
            # 1 -> 2 or 1 -> 4 or 3 -> 4 abort and allow to continue
            #
            # Scenario2: Principals, paths or accesses added after policy creation
            # 1 -> 6 no correction needed, allow to continue 
            #
            # Scenario3: Principals, paths or access removed after policy creation 
            # 1 -> 5 abort, apply remove and resync policy
            # 
            # Scenario4: Add principals, paths and then immediately remove them (add to remove ie. no permission string) (full or partial)
            # 6 -> 5 or 10 -> 9 where principals/paths are the same then abort and allow remove to continue else abort, remove and resync
            # 
            # Scenario5: Add access then immediately remove them (modify to modify)
            # 8 -> 7 Abort and resync
            #
            ###########################################
            appname = 'applyPolicies'
            #params = urllib.parse.quote_plus(connxstr+'Database='+dbname +';')
            cursor = cnxn.cursor()
            cursorinner = cnxn.cursor()
            transcursor = cnxn.cursor()
            #transfixcursor = cnxn.cursor()
            now =  datetime.datetime.utcnow()
            progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
            new_trans_status = 'Pending'

 
            # Scenario1
            policyCorrection(cursor,'1','4,2',False)
            policyCorrection(cursor,'3','4',False)
            # Scenario3
            policyCorrection(cursor,'1','5',True)
            # Scenario4
            policyCorrection(cursor,'6','5',True) # need to cater for partial
            policyCorrection(cursor,'10','9',True)
            # Scenario5
            policyCorrection(cursor,'8','7',True)

            ######################################
            # Duplicate detection
            #####################################
            duplicate_trans ="""with trans as (
                                select t.id,t.policy_id, t.repositoryName, o.id other_trans_id,o.policy_id other_trans_pol_id,o.repositoryName other_trans_repo_name
                                from policy_transactions t inner join policy_transactions o on t.adl_path = o.adl_path and t.trans_action = o.trans_action and t.acentry = o.acentry and t.id <> o.id and t.trans_status = 'Validate' and o.trans_status='Validate'),
                                mintrans as (select min(id) min_id from trans)
                                update poltrans  set trans_status ='Duplicate',trans_reason = concat('Identified as duplicate of transaction',(select min_id from mintrans),' for policy ',t.policy_id,' from repository ',t.repositoryName) 
                                from policy_transactions poltrans inner join trans  t on t.id = poltrans.id where t.id <> (select min(i.id) from trans i) """
            logging.info("Looking for any transaction duplicates...")
            transcursor.execute(duplicate_trans)
            
            #################################################################
            # 
            # Policy conflict detection (Rule of maximum)      
            # 
            # ###############################################################     
            #  determine which changes have matches to paths and principals elsewhere. if any fetch the paths, principals and policy ID(s) of the other policy/ies (in comma separated notation). The latter will be stored in the trans reason.
            # TODO add the inverse                                 union 
                                #select distinct t.id,t.policy_id, t.trans_type, t.adl_path,  t.principalval, t.principals_included, stuff((select distinct ',' + cast(ps.id as varchar) for xml path ('')),1,1,'') as policylist, t.storage_url,principal_type,permission_json from trans t inner join policy_snapshot_by_path ps on  ps.adl_path like t.adl_path+'%'  and t.principalval = ps.principal and t.policy_id <> ps.id
            policy_conflicts = """with trans as (
                                select id,policy_id, trans_type, storage_url adl_path, principalexp.value principalval, principals_included,storage_url, 'userList' principal_type, permission_json,repositoryName from policy_transactions pt cross apply OPENJSON(principals_included) with (users nvarchar(max) '$.userList' as json) cross APPLY OPENJSON(users) principalexp where trans_status = '1Validate')
                                select distinct t.id,t.policy_id, t.trans_type, t.adl_path,  t.principalval, t.principals_included, max(ps.id) as policylist, t.storage_url,principal_type,permission_json,t.repositoryName from trans t inner join policy_snapshot_by_path ps on t.storage_url = replace(replace(ps.adl_path,'[''',''),''']','')   and t.principalval = ps.principal and concat(t.policy_id,t.repositoryName) <> concat(ps.id,ps.repositoryName)
                                group by t.id,t.policy_id, t.trans_type, t.adl_path,  t.principalval, t.principals_included, t.storage_url,principal_type,permission_json,t.repositoryName """

            # This query needs some explaination... it is broken into 3 parts. 
            # 1st part (trans) explodes all in-flight transactions by principal and path. 
            # 2nd part (transconflict) checks to see whether that principal has other policies for that path by doing a distinct count of repository name and policy ID
            # 3rd part (maxperms) calculates the maximum permissions for all policies for that path for that principal
            # final part converts the results from maxperms into ADLS permissions rwx and does a lookup for the AAD OID in the cache table
            policy_optimiser = """
                           with trans as
                            (select id,policy_id, adl_permission_str, trans_type, adl_path, principalexp.value principalval, principals_included,storage_url, 'user' principal_type, permission_json, trans_status from policy_transactions pt cross apply OPENJSON(principals_included) with (users nvarchar(max) '$.userList' as json) cross APPLY OPENJSON(users) principalexp
                            where trans_status ='Validate'
                            union
                            select id,policy_id, adl_permission_str, trans_type, adl_path, principalexp.value principalval, principals_included,storage_url, 'group' principal_type, permission_json, trans_status from policy_transactions pt cross apply OPENJSON(principals_included) with (groups nvarchar(max) '$.groupList' as json) cross APPLY OPENJSON(groups) principalexp
                            where trans_status ='Validate'
                            ),
                            transconflict as
                            (select tt.storage_url,tt.principal_type,pth.principal,min(tt.id) min_id,count(distinct concat(pth.ID,pth.RepositoryName)) totalstrans 
                            from policy_snapshot_by_path pth inner join trans tt on tt.storage_url like pth.adl_path+'%'  and tt.principalval = pth.principal and tt.principal_type = pth.principal_type group by tt.storage_url,pth.principal,tt.principal_type having count(distinct concat(pth.ID,pth.RepositoryName))>1)
                            select a.AAD_OID,t.principal,t.principal_type,t.storage_url,min(p.id) min_policy_id,
                            concat(case when max(case when m.adls_perm='r' then 1 else 0 END) =1 then 'r' else '-' end,
                            case when max(case when m.adls_perm='w' then 1 else 0 end) = 1 then 'w' else '-' end, 
                            case when max(case when m.adls_perm='x' then 1 else 0 end) = 1 then  'x' else '-' end) adlsperm 
                            from policy_snapshot_by_path p
                            inner join perm_mapping m on p.permission = m.ranger_perm
                            inner join transconflict t on t.storage_url  like p.adl_path+'%' and t.principal = p.principal
                            inner join aad_cache a on concat(substring(t.principal_type,1,1) ,t.principal) = a.aad_principal_name
                            group by a.AAD_OID,t.principal, t.principal_type,t.storage_url
                            """

            #logging.info(policy_conflicts)
            trans_id = -1
            cursor.execute(policy_optimiser)
            transactions = cursor.fetchall()
            for transaction_row in transactions: #loop through the transaction above principal by principal where a potential conflict is found
                    logging.info("Found policy transactions in conflict, removing principal "+ transaction_row[1] + " for storage url "+ transaction_row[3])
                    trans_update ="""update policy_transactions set principals_excluded = trim(',' FROM principals_excluded+',""" + transaction_row[1] + """'), trans_status='Recalculate', 
                                     trans_reason=concat(trans_reason,'... Principal """ + transaction_row[1] + """ excluded due to policy conflict. Recalculation of ACE entry required ...')
                    where  storage_url = '""" + transaction_row[3] + """' and acentry like '%""" + transaction_row[0] + """%' and trans_status = 'Validate'"""
                    logging.info(trans_update)
                    transcursor.execute(trans_update)
                    lPrincipalsIncluded[str(transaction_row[2]) + 'List'].append(transaction_row[1]) # used to populate the principals included field even though it is just a single pincipal
                    spids[transaction_row[2]].append(transaction_row[0])
                    permstr = transaction_row[5]
                    acentry = spidsToACEentry(spids,permstr)
                    transStatus = 'Pending'
                    transReason = 'Transaction auto-generated due to policy conflicts (rule of maximum) for principal '+transaction_row[1] + ', path ' + transaction_row[2] + ', based on policy '+  str(transaction_row[3]) 
                    captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
                    trans_insert = "insert into " + dbschema + ".policy_transactions (policy_id, repositoryName, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason,adl_permission_str,permission_json, principals_included) " \
                    " values ('" + str(transaction_row[4]) + "','Global','" + transaction_row[3]  + "','" + transaction_row[3] + "','" + 'setAccessControlRecursive' + "','" + 'modify' + "','" + acentry + "','"+ captureTime + "','7','" + transStatus + "','" + transReason + "','" + permstr+ "','','" + json.dumps(lPrincipalsIncluded) +"')"
                    ## Future optimisation, adapt capturetransaction function to access trans_reason and ensure all global variables are reset
                    ##captureTransaction(cursor,'setAccessControlRecursive','modify', hdfsentry,spids,pPolicyId,permstr,7,'','Global')          

                    logging.info(trans_insert)
                    transcursor.execute(trans_insert)
                    lPrincipalsIncluded[str(transaction_row[2]) + 'List'].remove(transaction_row[1]) # was temporarily assigned for storing the principals_included column above, now reset for the next iteration of this for loop

            trans_update = """update policy_transactions set trans_status ='"""+ new_trans_status + """' where trans_status='Validate'"""
            #logging.info(trans_update)
            logging.info("Updating any other transactions not in conflict awaiting validation...")
            transcursor.execute(trans_update)
            cnxn.commit()
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
    #graphtoken = getBearerToken(tenantid,"graph.microsoft.com",spnid,spnsecret)
    try:
            cursor = cnxn.cursor()
            transcursor = cnxn.cursor()
            policy_recalc = """select id,principals_excluded,principals_included, adl_permission_str from policy_transactions where trans_status = 'Recalculate'"""
            cursor.execute(policy_recalc)
            transactions = cursor.fetchall()
            for transaction_row in transactions:
              logging.info(json.dumps(transaction_row[2]))
              principals_json = json.loads(transaction_row[2])

              #principals_json[transaction_row[8]].remove(transaction_row[4])
              try:
                logging.info(principals_json['userList'])
                for userentry in principals_json['userList']: #will be grouplist next time round
                    for excludeprincipal in transaction_row[1].split():
                        if userentry == excludeprincipal:
                            principals_json['userList'].remove(excludeprincipal)
              except KeyError:
                  None
              try:
                logging.info(principals_json['groupList'])
                for groupentry in principals_json['groupList']: 
                    for excludeprincipal in transaction_row[1].split(): 
                        if groupentry == excludeprincipal:
                            principals_json['groupList'].remove(excludeprincipal)
              except KeyError:
                  None
              try: 
                  total_users = len(principals_json['userList'])
                  usersIncluded = principals_json['userList']
              except KeyError:
                  total_users = 0
                  usersIncluded = None
              try:
                  total_groups = len(principals_json['groupList'])
                  groupsIncluded = principals_json['groupList']
              except KeyError:
                  total_groups = 0
                  groupsIncluded = None
              total_principals = total_users + total_groups
              logging.info('total principals '+str(total_principals))

              if total_principals>0:
                spids = getSPIDs(usersIncluded,groupsIncluded)                  
                acentry = spidsToACEentry(spids,transaction_row[3])   
                logging.info("Ace entry is now : "+acentry)          
                trans_update = """update policy_transactions set trans_status = 'Pending', principals_included ='""" + json.dumps(principals_json) + """', trans_reason = concat(trans_reason,'... ACE entry recalculated. '),acentry = '""" + acentry + """' where id = """ + str(transaction_row[0])
                logging.info("Process recalc update: "+trans_update)
                transcursor.execute(trans_update)
              else: # no principals remaining
                trans_update = """update policy_transactions set trans_status = 'Ignored', principals_included ='""" + json.dumps(principals_json) + """', trans_reason = concat(trans_reason,'... Transaction ignored as all principals were excluded due to policy conflicts.'),acentry = '' where id = """ + str(transaction_row[0])
                logging.info("Process recalc update: "+trans_update)
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

def reSyncPolicy(policyID,repoName):
    logging.info("Resyncing policy... " + str(policyID) + " - " + repoName)
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    cursor = cnxn.cursor()
    now =  datetime.datetime.utcnow()
    progstarttime = now.strftime('%Y-%m-%d %H:%M:%S')
    # note about the convert statements below, this is merely to convert the time value into the correct format for the fn_cdc_map_time_to_lsn function.
    # either there will be no data in the ctl table (first run) and then all changes are scanned. otherwise there is a last checkpoint found. if this matches the maximum lsn of the database then no changes have happened since the last run ie do nother. otherwise scan for changes...
    sql_txt = """select [id],[RepositoryName],[Name],coalesce(resources,paths) Resources,[Status],replace(permMapList,'''','"') permMapList,[Service Type],tables,table_type,table_names from ranger_policies where id = """ + str(policyID) + """ and repositoryName = '""" + str(repoName) + """' """

    logging.info(sql_txt)
    cursor.execute(sql_txt)
    row = cursor.fetchone()


    if row[4] in ('Enabled','True') : 
        logging.info("Enabled, calling syncpolicy")
        syncPolicy(cursor,row[0],row[3],row[5],row[9],row[8],row[7],row[1])    
        cnxn.commit()

        '''
        # obtain all the comma separated resource paths and make one ACL call path with a dictionary of groups and users, and a set of rwx permissions
        hdfsentries = row[3].strip("path=[").strip("[").strip("]").split(",")

        #Load the json string of permission mappings into a dictionary object
        permmaplist = json.loads(row[5])
        tableNames = json.loads(row[9])

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

            if row[8] == 'Exclusion' and row[7] != '*': # process at table level
                logging.warning("***** Table exclusion list in policy detected")
                tablesToExclude = row[7].split(",")
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
                        #captureTransaction(cursor,'setAccessControlRecursive','modify', tableNames[tblindb],spids,row.id,permstr,1,permap["permList"])
                        #trans_insert = "insert into " + dbschema + ".policy_transactions (policy_id, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason) " \
                        #    " values ('" + str(transaction_row[0]) + "','" + transaction_row[7]  + "','" + transaction_row[3] + "','" + 'setAccessControlRecursive' + "','" + 'modify' + "','" + acentry + "','"+ captureTime + "','7','" + transStatus + "','" + transReason + "')"
                        #logging.info(trans_insert)
                        #transcursor.execute(trans_insert)


                # if not a match to tables in the exclusion list then 
                # captureTransaction(cursor,'setAccessControlRecursive','modify', #pathToTable,spids,row.id,permstr,1,permap["permList"])                               
            else: #capture entry as normal at the database level
                for hdfsentry in hdfsentries:
                    hdfsentry = hdfsentry.strip().strip("'")
                    logging.info("Passing path: " + hdfsentry)
                    #captureTransaction(cursor,'setAccessControlRecursive','modify', hdfsentry,spids,row.id,permstr,1,permap["permList"])
                    trans_insert = "insert into " + dbschema + ".policy_transactions (policy_id, storage_url,adl_path, trans_action,trans_mode, acentry,date_entered,trans_type,trans_status,trans_reason) " \
                    " values ('" + str(transaction_row[0]) + "','" + transaction_row[7]  + "','" + transaction_row[3] + "','" + 'setAccessControlRecursive' + "','" + 'modify' + "','" + acentry + "','"+ captureTime + "','7','" + transStatus + "','" + transReason + "')"
                    logging.info(trans_insert)
                    transcursor.execute(trans_insert)
            '''



devstage = 'live'
#getPolicyChanges()
#businessRuleValidation()
#processRecalc()
#storeQueueItems() 
## some logic for rule of max
##if removing a user then check whether other perms should still apply and remove just the delta ie if removing a user but has read elsewhere then ignore the user for this specific transaction and capture a transaction to just remove the os.read
##if removing a permission check whether that permission if found in another policy and if so ignore
