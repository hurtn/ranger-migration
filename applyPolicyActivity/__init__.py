# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import os
import logging
import requests
import json
import azure.functions as func
import azure.durable_functions as df
import asyncio

from time import perf_counter
from applyPolicies import AclUpdaterService as aclService
from collections import namedtuple

async def main(payload:str) -> str:
            
    acentry = ""

    counter = 0
    
    startTimer = perf_counter()

    for sp in payload['SPIDs']:

        for spid in payload['SPIDs'][sp]:

            missingBit = 'user::r--,group::r--,other::r--'

            # +',default:'+sp+':'+spid + ':'+permissions +missingBit
            acentry = sp+':'+spid + ':'+payload['Permissions'] + ','+missingBit

            print(payload['AdlPath'] + ' -> '+acentry)

            counter += await aclService.override_bulk_recursivly(adlPath=payload.ad, acl=acentry)
    
    stopTimer = perf_counter()

    print(f'Completed in {stopTimer-startTimer:.3f}')

    return str(counter)