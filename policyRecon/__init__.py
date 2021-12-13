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
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)


def main(mytimer: func.TimerRequest, msg: func.Out[typing.List[str]]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    devstage=os.environ["stage"]
    topLevelRecon()

def topLevelRecon():


    connxstr=os.environ["DatabaseConnxStr"]
    dbname = os.environ["dbname"]
    dbschema = os.environ["dbschema"]
    cnxn = pyodbc.connect(connxstr)
    batchsize = 200
    params = urllib.parse.quote_plus(connxstr)

    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str,echo=False)

    # sql alchemy listener
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    connxstr=os.environ["DatabaseConnxStr"]
    cnxn = pyodbc.connect(connxstr)

    sqlpaths = "select distinct adl_path from policy_snapshot_by_path"
    cursor = cnxn.cursor()
    cursor.execute(sqlpaths)
    show_db = cursor.fetchall()
    acls = defaultdict(list)
    allpermsdf = pd.DataFrame()
    counter=0
    if show_db:
        for d in show_db:
            pathprefix = d[0][0:d[0].find(d[0].split("/")[3]+'/')+len(d[0].split("/")[3]+'/')]
 
            account_name = d[0].split("/")[2][0:d[0].split("/")[2].find('.')]
            account_key =""
 
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https",account_name), credential=account_key)
            counter += 1
            filesystem_client = service_client.get_file_system_client(file_system=d[0].split("/")[3])
            directory_client = filesystem_client.get_directory_client(d[0][d[0].find(d[0].split("/")[3]+'/')+len(d[0].split("/")[3]+'/'):])
            acl_props = directory_client.get_access_control()


            for i in acl_props['acl'].split(","):
                    if (i.split(":")[0])== 'default':
                                if (i.split(":")[2]) not in ('user','group','mask','other',''): 
                                    None
                    else:
                        if (i.split(":")[1]) not in ('user','group','mask','other',''): 
                            for perm in i.split(":")[2]:
                                if perm != '-':  acls[d[0][d[0].find(d[0].split("/")[3]+'/')+len(d[0].split("/")[3]+'/'):]].append([i.split(":")[1],perm])
            df = pd.DataFrame(list(acls.items()), columns = ['path','ace'] )
            df = df.explode('ace')
            df['fullpath'] = pathprefix + df.path.astype(str)


            df = df.set_index('path')
            df = pd.concat([df.drop(columns='ace'),pd.DataFrame(df['ace'].tolist(), index=df.index,columns=['principal','permission'])],axis=1)
            allpermsdf = allpermsdf.append(df)
    print(tabulate(allpermsdf, headers='keys', tablefmt='presto'))
    allpermsdf.to_sql("adlsperms",engine,index=False,if_exists="replace")

    sqltext = """ insert into  recon_report (	[audit_timestamp],
                    [adl_path],
                    [principal],
                    [aad_oid],
                    [adls_perm],
                    [fullpath],
                    [Inconsistency]) 
                select current_timestamp audit_timestamp,*,'Missing permissions not assigned' Inconsistency 
                from (
                select adl_path, pth.principal,
                aad.aad_oid, map.adls_perm,perm.fullpath 
                from policy_snapshot_by_path pth 
                    inner join perm_mapping map on pth.permission= map.ranger_perm 
                    inner join aad_cache aad on pth.principal = aad.aad_display_name 
                    left outer join adlsperms perm on pth.adl_path = perm.fullpath) a where fullpath is null
                UNION
                select current_timestamp, *,'Unexpected permissions assigned' msg from (
                select perm.fullpath, pth.principal,
                aad.aad_oid, perm.permission,pth.adl_path
                from adlsperms perm 
                    inner join aad_cache aad on perm.principal = aad.aad_oid 
                    left outer join policy_snapshot_by_path pth  on  perm.fullpath = pth.adl_path and aad.aad_display_name = pth.principal
                ) a where adl_path is null
        """
    cursor.execute(sqltext)


devstage = 'live'
