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
from azure.core.exceptions import AzureError
from azure.storage.filedatalake.aio import (
    DataLakeServiceClient,
)
from azure.identity.aio import ClientSecretCredential #DefaultAzureCredential 

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
    logging.info("Dequeued item in poison queue: "+ json.dumps(result))
    tenantid=os.environ["tenantID"]
    spnid= os.environ["SPNID"]
    spnsecret= os.environ["SPNSecret"]
    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    cursor = cnxn.cursor()
    now =  datetime.datetime.utcnow()
    acls_changed = 0
    vContinuationToken = ''
    vContinuationMsg = 'Recovered from poison queue. Awaiting retry...'
    captureTime = now.strftime('%Y-%m-%d %H:%M:%S')
    queue_upd = "update " + dbschema + ".policy_transactions set trans_status = 'Poisoned',  trans_reason = concat(trans_reason, '" + vContinuationMsg + "'), last_updated = '"+ captureTime + "' where id = "+str(result["id"])
    logging.info(queue_upd)
    cursor.execute(queue_upd)
    cnxn.commit()

devstage = 'live'