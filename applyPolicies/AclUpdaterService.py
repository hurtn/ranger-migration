import asyncio
from datetime import datetime
import os
import uuid
import sys
import re
import random

from time import time
from typing import Any

from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake._models import AccessControlChangeResult
from azure.core.exceptions import AzureError
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings


#  Alterantive way of connecting to the service client
# def initialize_storage_account_ad(storage_account_name, client_id, client_secret, tenant_id):

#     try:
#         global service_client

#         credential = ClientSecretCredential(tenant_id, client_id, client_secret)

#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=credential)

#     except Exception as e:
#         print(e)


def initialize(storageName, accountKey):

    try:

        global service_client

        service_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", storageName), credential=accountKey)

        if (service_client != None):
            print('AclUpdaterService client initialized')

    except Exception as e:
        print(e)


# When you set an ACL, you replace the entire ACL including all of it's entries.
# If you want to change the permission level of a security principal or add a new security principal
# to the ACL without affecting other existing entries, you should update the ACL instead. 
async def override_bulk_recursivly(adlPath, acl):

    try:

        if (service_client != None):

            directory_client = get_directory_client_from_path(adlPath)

            #await create_child_files(directory_client=directory_client, num_child_files=100)

            print(directory_client.url)

            acl_props = directory_client.get_access_control()

            # set the permissions of the entire directory tree recursively
            # update/remove acl operations are performed the same way
            failed_entries = []

            # the progress callback is invoked each time a batch is completed
            def progress_callback(acl_changes):
                print(("In this batch: {} directories and {} files were processed successfully, {} failures were counted. " +
                       "In total, {} directories and {} files were processed successfully, {} failures were counted.")
                      .format(acl_changes.batch_counters.directories_successful, acl_changes.batch_counters.files_successful,
                              acl_changes.batch_counters.failure_count, acl_changes.aggregate_counters.directories_successful,
                              acl_changes.aggregate_counters.files_successful, acl_changes.aggregate_counters.failure_count))

                # keep track of failed entries if there are any
                failed_entries.append(acl_changes.batch_failures)

            # illustrate the operation by using a small batch_size
            try:
                acl_change_result = directory_client.set_access_control_recursive(acl=acl,
                                                                                        progress_hook=progress_callback,
                                                                                        batch_size=20)

            except AzureError as error:
                # if the error has continuation_token, you can restart the operation using that continuation_token
                print('AzureError ' + error.message +
                      ' ACL ' + acl + ' path ' + adlPath)

                if error.continuation_token:
                    acl_change_result = \
                        directory_client.set_access_control_recursive(acl=acl,
                                                                            continuation_token=error.continuation_token,
                                                                            progress_hook=progress_callback,
                                                                            batch_size=20)

            print("Summary: {} directories and {} files were updated successfully, {} failures were counted."
                  .format(acl_change_result.counters.directories_successful, acl_change_result.counters.files_successful,
                          acl_change_result.counters.failure_count))

            # if an error was encountered, a continuation token would be returned if the operation can be resumed
            if acl_change_result.continuation is not None:
                print("The operation can be resumed by passing the continuation token {} again into the access control method."
                      .format(acl_change_result.continuation))

            # get and display the permissions of the parent directory again
            acl_props = directory_client.get_access_control()

            print("New permissions of directory '{}' and its children are {}.".format(
                adlPath, acl_props['permissions']))

            return (int(acl_change_result.counters.files_successful) + int(acl_change_result.counters.directories_successful))

    except Exception as e:
        print(e)


def update_bulk_recursively(adlPath, acl):
    
    try:

        directory_client = get_directory_client_from_path(adlPath)

        directory_client.update_access_control_recursive(acl=acl)

        acl_props = directory_client.get_access_control()
        
        print(acl_props['permissions'])

    except Exception as e:
        print(e)


def remove_bulk_recursively(adlPath, acl):
    
    try:

        directory_client = get_directory_client_from_path(adlPath)

        directory_client.remove_access_control_recursive(acl=acl)

    except Exception as e:
        print(e)


def get_file_system_from_path(path):

    return re.split('/', path)[1]


def get_directory_client_from_path(path):

    file_system = get_file_system_from_path(path)

    file_system_client = service_client.get_file_system_client(file_system)
    
    directory_client = file_system_client.get_directory_client(directory=str(path).replace(file_system, ''))

    return directory_client


# Create dummy sub-folders
async def create_child_files(directory_client, num_child_files):

    import itertools    
    
    async def create_file():
        # generate a random name
        file_name = str(uuid.uuid4()).replace('-', '')
        file_client = directory_client.get_file_client(file_name)
        await file_client.create_file()

    futures = [asyncio.ensure_future(create_file())
               for _ in itertools.repeat(None, num_child_files)]
    await asyncio.wait(futures)
    print("Created {} files under the directory '{}'.".format(
        num_child_files, directory_client.path_name))
