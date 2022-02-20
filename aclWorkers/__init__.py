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

from email.policy import default
import os
import datetime
import logging
import urllib
import pyodbc
import sys
import azure.functions as func
import json
from time import perf_counter 
import requests,uuid
from requests.auth import HTTPBasicAuth
import asyncio
from azure.core.exceptions import AzureError, HttpResponseError
from azure.storage.filedatalake.aio import (
    DataLakeServiceClient,
)
from azure.identity.aio import ClientSecretCredential #used for service principal based auth
from azure.identity.aio import DefaultAzureCredential #used for managed identity based auth



async def main(msg: func.QueueMessage):
    logging.info('Python queue trigger function processed a queue item.')
    #eventloop = asyncio.get_event_loop()

    result = json.dumps({
        'id': msg.id,
        'body': msg.get_body().decode('utf-8'),
        'expiration_time': (msg.expiration_time.isoformat()
                            if msg.expiration_time else None),
        'insertion_time': (msg.insertion_time.isoformat()
                           if msg.insertion_time else None),
        'time_next_visible': (msg.time_next_visible.isoformat()
                              if msg.time_next_visible else None),
        'pop_receipt': msg.pop_receipt,
        'dequeue_count': msg.dequeue_count
    })
    result =  json.loads(msg.get_body().decode('utf-8'))
    logging.info("..............Dequeued: "+ json.dumps(result))
    tenantid=os.environ["tenantID"]
    spnid= os.environ.get('SPNID','')
    spnsecret= os.environ.get('SPNSecret','')
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    cursor = cnxn.cursor()
    now =  datetime.datetime.utcnow()
    acls_changed = 0
    vContinuationToken = ''
    vContinuationMsg = ''
    captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
    queue_itm = "select trans_status, continuation_token from " + dbschema + ".policy_transactions where id = "+str(result["id"])
    cursor.execute(queue_itm)
    transrow = cursor.fetchone()
    if transrow:
            logging.info('De-queued transaction ID '+ str(result["id"]) + ' with transaction status ' + str(transrow[0]) + '. Checking for continuation token... ') 

            if transrow[1]:  # if a continuation token exists
                vContinuationToken = transrow[1]
                vContinuationMsg = 'Recovering with continuation token' + vContinuationToken

            if vContinuationToken != '':
                logging.info('Continuation token =' +  str(vContinuationToken))

            if transrow[0] not in ('Abort','Aborted'): # if not in an aborted state then mark as de-queued
                queue_upd = "update " + dbschema + ".policy_transactions set trans_status = 'De-queued',  trans_reason = concat(trans_reason, '" + vContinuationMsg + "'), last_updated = '"+ captureTime + "' where trans_status <>  'Aborted' and trans_status <> 'Abort' and id = "+str(result["id"])
                logging.info(queue_upd)
                cursor.execute(queue_upd)
                cnxn.commit()
                #if len(recsupdated)>0:
                try:
                    u1_start = perf_counter()         

                    #storagetoken = getBearerToken(tenantid,"storage.azure.com",spnid,spnsecret)
                    #acls_changed += setADLSBulkPermissions(storagetoken, str(result["storage_url"]), str(result["acentry"]),str(result["trans_action"]),str(result["trans_mode"]))

                    if spnid!='' and spnsecret!='': # use service principal credentials
                        default_credential = ClientSecretCredential(            tenantid,            spnid,            spnsecret,       )
                    else: # else use managed identity credentials
                        default_credential = DefaultAzureCredential() 

                    urlparts =  str(result["storage_url"]).split('/',4)
                    service_client = DataLakeServiceClient("https://{}".format(urlparts[2]),
                                                                credential=default_credential)

                    logging.info("Obtained service client")
                    async with service_client:
                        filesystem_client = service_client.get_file_system_client(file_system=urlparts[3])
                        logging.info('Setting ACLs recursively ' + str(result['acentry']))
                        acls_changed = await set_recursive_access_control(filesystem_client,urlparts[4], str(result["acentry"]),result["id"],u1_start, str(result["trans_mode"]), vContinuationToken)
                        #logging.info("No ACL changes = "+ str(acls_changed))
                        await filesystem_client.close()
                        await service_client.close()  
                        await default_credential.close()
                        now =  datetime.datetime.utcnow()
                        captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
                        u1_stop = perf_counter()

                        if isinstance(acls_changed,str):
                            acls_reason = acls_changed
                            acls_changed=0
                            queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'Error', acl_count = "+str(acls_changed) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason, "+ str(acls_reason) + ". Finished in  " + str(format(u1_stop-u1_start,'.3f')) + " seconds. ') where id = "+str(result["id"])

                        elif not acls_changed or acls_changed <0: # there were either no files in the folder or some error or aborted due to user error
                            if not acls_changed or acls_changed == 0:
                                    acls_changed=0
                                    queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'Warning', acl_count = "+str(acls_changed) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason,'Completed but did not set any ACLS. This may be due to an empty folder. Finished in  " + str(format(u1_stop-u1_start,'.3f')) + " seconds. ') where id = "+str(result["id"])
                            elif acls_changed == -4:
                                    queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'Aborted', acl_count = "+str(acls_changed) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason,'Aborted due to user error correction in  " + str(format(u1_stop-u1_start,'.3f')) + " seconds. ') where id = "+str(result["id"])
                            else:
                                    queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'Error', acl_count = "+str(acls_changed) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason,'Aborted in  " + str(format(u1_stop-u1_start,'.3f')) + " seconds. ') where id = "+str(result["id"])
                        else:
                            queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'Done', acl_count = "+str(acls_changed) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason,'Completed in " + str(format(u1_stop-u1_start,'.3f')) + " seconds. ') where id = "+str(result["id"])
                        logging.info("!!!!! Queue update SQL: "+queue_comp)
                        cursor.execute(queue_comp)
                        cnxn.commit()

                        
                except pyodbc.DatabaseError as err:
                    cnxn.commit()
                    sqlstate = err.args[1]
                    sqlstate = sqlstate.split(".")
                    logging.error('Error message: '.join(sqlstate))
                #except Exception as e:
                #    logging.error('Error in line ' + str(sys.exc_info()[-1].tb_lineno) + ' occured when trying to process ACL work items:')
                #    queue_upd = "update " + dbschema + ".policy_transactions set trans_status = 'Queued', trans_reason = concat(trans_reason,'Requeuing due to error: " + e + "') where id = "+str(result["id"])
                #    cursor.execute(queue_upd)
                #    cnxn.commit()
                #    #potentially try to add the item back on the queue here
                else:
                    cnxn.commit()
                    #print('Done')
                finally:
                    cnxn.autocommit = True
            else:
                logging.error('Transaction ' + str(result["id"]) + ' in Abort state. Ignoring...') # This item will be removed from the queue as no error has been thrown i.e. will not end up in poison queue
                
    else:
        logging.error('Could not find transaction record with ID ' + str(result["id"]) + '. Please contact support')



def getBearerToken(tenantid,resourcetype,spnid,spnsecret):
    endpoint = 'https://login.microsoftonline.com/' + tenantid + '/oauth2/token'

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = 'grant_type=client_credentials&client_id='+spnid+'&client_secret='+ spnsecret + '&resource=https%3A%2F%2F'+resourcetype+'%2F'
    #payload = 'resource=https%3A%2F%2F'+resourcetype+'%2F'
    #print(endpoint)
    #print(payload)
    r = requests.post(endpoint, headers=headers, data=payload)
    response = r.json()
    print("Obtaining AAD bearer token for resource "+ resourcetype + "...")
    try:
      bearertoken = response["access_token"]
    except KeyError:
      print("Error obtaining bearer token: "+ response)
    #print(bearertoken)
    print("Bearer token obtained.\n")
    return bearertoken

# a variation of the function above which access a dictionary object of users and groups so that we can set the ACLs in bulk with a comma seprated list of ACEs (access control entries)
def setADLSBulkPermissions(aadtoken, adlpath, acentry, trans_action, trans_mode,lcursor, lcnxn):
        # Read documentation here -> https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    if aadtoken and acentry and adlpath and trans_action and trans_mode:
        puuid = str(uuid.uuid4())
        headers = {'x-ms-version': '2019-12-12','Authorization': 'Bearer %s' % aadtoken, 'x-ms-acl':acentry,'x-ms-client-request-id': '%s' % puuid}
        request_path = adlpath+"?action="+ trans_action + "&mode=" + trans_mode
        print("Setting ACLs  " + acentry + " on " +adlpath + "...")
        t1_start = perf_counter() 
        if devstage == 'live':
            r = requests.patch(request_path, headers=headers)
            response = r.json()
            t1_stop = perf_counter()
            #print(r.text)
            if r.status_code == 200:
                logging.info("Response Code: " + str(r.status_code) + "\nDirectories successful:" + str(response["directoriesSuccessful"]) + "\nFiles successful: "+ str(response["filesSuccessful"]) + "\nFailed entries: " + str(response["failedEntries"]) + "\nFailure Count: "+ str(response["failureCount"]) + f"\nCompleted in {t1_stop-t1_start:.3f} seconds\n")  
                return(int(response["filesSuccessful"]) + int(response["directoriesSuccessful"]))
            else:
                logging.error("Error: " + str(r.text))
                raise Exception("Error while trying to set ACLs " + str(r.text))
                return(0)
        else:
            logging.info("Environment setting was set to non-prod therefore no ACLs have been set. "+ devstage)
            return(-2)
    else:
        logging.warning("Warning: Could not set ACLs as no users/groups were supplied in the ACE entry. This can happen when all users are either in the exclusion list or their IDs could not be found in AAD.")    
        return(-1)
    #aces = spntype+':'+spn+spnaccsuffix + ':'+permissions+',default:'+spntype+':'+spn+spnaccsuffix + ':'+permissions,'x-ms-client-request-id': '%s' % puuid

async def set_recursive_access_control(filesystem_client,dir_name, acl,transid,proc_start,trans_mode, pContinuationToken):
    # the progress callback is invoked each time a batch is completed 
    l1_start = perf_counter()
    user_error_abort = False
    
    async def progress_callback(acl_changes):
        global user_error_abort 
        user_error_abort = False
        logging.info(("Transaction " + str(transid) + " in progress. In this batch: {} directories and {} files were processed successfully, {} failures were counted. " +
                "In total, {} directories and {} files were processed successfully, {} failures were counted.")
                .format(acl_changes.batch_counters.directories_successful, acl_changes.batch_counters.files_successful,
                        acl_changes.batch_counters.failure_count, acl_changes.aggregate_counters.directories_successful,
                        acl_changes.aggregate_counters.files_successful, acl_changes.aggregate_counters.failure_count))
        connxstr=os.environ["DatabaseConnxStr"]
        dbname = os.environ["dbname"]
        dbschema = os.environ["dbschema"]
        lcnxn = pyodbc.connect(connxstr)
        lcursor = lcnxn.cursor()
        now =  datetime.datetime.utcnow()                        
        captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
        l1_stop = perf_counter()
        trans_status_check = "select trans_status from policy_transactions where trans_status = 'Abort' and id = "+str(transid)
        lcursor.execute(trans_status_check)
        aborttrans = lcursor.fetchall()
        if len(aborttrans) > 0:
            logging.error("!!! Aborting transaction in progress. ID =  "+ str(transid))
            user_error_abort = True
            raise Exception("Abort in progress transaction due to user correction")
        else:
            # if not in abort status then update with continuation token
            queue_comp = "update " + dbschema + ".policy_transactions set trans_status = 'InProgress', continuation_token = '" + str(acl_changes.continuation) + "',acl_count = "+str(acl_changes.aggregate_counters.files_successful) + ", last_updated = '"+ captureTime + "', trans_reason = concat(trans_reason,'Running for " + str(format(l1_stop-proc_start,'.3f')) + " seconds. ') where id = "+str(transid)
            logging.info("!!!!! Queue update un progress SQL: "+queue_comp)
            lcursor.execute(queue_comp)
            lcnxn.commit()
        # keep track of failed entries if there are any
        failed_entries.append(acl_changes.batch_failures)

    # illustrate the operation by using a small batch_size
    #acl_change_result = ""
    try:
        #acls = 'user::rwx,group::r-x,other::rwx'
        #acls = 'default:user:1ad1af70-791f-4d61-8bf1-27ccade3342a:rw-,default:user:9e501fc2-c687-4ba5-bfb9-b8afa948cb83:rw-,default:user:02b60873-3213-46aa-8889-8866e693d559:rw-'
        failed_entries = []

        #dir_name = "sample"
        #logging.info("Raw directory named '{}'.".format(dir_name))
        #logging.info("Clean directory named '{}'.".format(urllib.parse.unquote(dir_name)))
        #directory_client = await filesystem_client.create_directory(dir_name)
        #dir_name = 'base1/nyctaxidata/green'
        directory_client = filesystem_client.get_directory_client(dir_name)
        if trans_mode == 'modify':
            if pContinuationToken != '':
                acl_change_result = await directory_client.update_access_control_recursive(acl=acl,
                                                                                        continuation_token = pContinuationToken,
                                                                                        progress_hook=progress_callback,
                                                                                        batch_size=2000)
            else:
                acl_change_result = await directory_client.update_access_control_recursive(acl=acl,
                                                                                        progress_hook=progress_callback,
                                                                                        batch_size=2000)
        elif trans_mode == 'remove':
            if pContinuationToken != '':
                acl_change_result = await directory_client.remove_access_control_recursive(acl=acl,
                                                                                        continuation_token = pContinuationToken,
                                                                                        progress_hook=progress_callback,
                                                                                        batch_size=2000)
            else:
                acl_change_result = await directory_client.remove_access_control_recursive(acl=acl,
                                                                                        progress_hook=progress_callback,
                                                                                        batch_size=2000)

        else:
          logging.error('Error during setting ACLs recursively for transaction '+str(transid) + ' due to unknown transaction mode ' + trans_mode)
          return -2


        await directory_client.close()
        logging.info("Result:" + str(acl_change_result))
        logging.info("Summary: {} directories and {} files were updated successfully, {} failures were counted."
                .format(acl_change_result.counters.directories_successful, acl_change_result.counters.files_successful,
                        acl_change_result.counters.failure_count))

        #if an error was encountered, a continuation token would be returned if the operation can be resumed
        if acl_change_result.continuation is not None:
            logging.info("The operation can be resumed by passing the continuation token {} again into the access control method."
                    .format(acl_change_result.continuation))
        return acl_change_result.counters.files_successful
    except  HttpResponseError as error:    
        logging.error("Error when attempting to set the following acl "+acl + " on " + dir_name + " :" + str(error))
        return "Error when attempting to set the following acl "+acl + " on " + dir_name + " :" + str(error)
    except AzureError as error:
        #logging.ERROR("aclWorkers: SDK Error whilst setting ACLs recursively") # "  + str(error) ) #+ " at line no " + str(sys.exc_info()[-1].tb_lineno))
        # if the error has continuation_token, you can restart the operation using that continuation_token
        if error.continuation_token:
            if trans_mode == 'modify':
                acl_change_result = \
                    await directory_client.update_access_control_recursive(acl=acl,
                                                                        continuation_token=error.continuation_token,
                                                                        progress_hook=progress_callback,
                                                                        batch_size=2000)
            elif trans_mode == 'remove':       
                    await directory_client.remove_access_control_recursive(acl=acl,
                                                            continuation_token=error.continuation_token,
                                                            progress_hook=progress_callback,
                                                            batch_size=2000)                                                                 
        else: None                                                            
        await directory_client.close()
 
    except Exception as e:
        logging.error('Error during setting ACLs recursively for transaction '+str(transid) + ' due to ' + str(e) + ' at line no ' + str(sys.exc_info()[-1].tb_lineno) )
        if user_error_abort: 
            return -4
        else: 
            return -2


    # get and display the permissions of the parent directory again
    #await directory_client.close()
    #acl_props = await directory_client.get_access_control()
    #logging.info("New permissions of directory '{}' and its children are {}.".format(dir_name, acl_props['permissions']))



devstage = 'live'