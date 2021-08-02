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

import logging

import numpy as np
import pandas as pd
from tabulate import tabulate
import sys,os
sys.path.append( 'C:\workspace\centrica' )
from storePolicies.hive_ms import fetch_hive_dbs
from storePolicies.ranger import fetch_ranger_hive_dbs


# Parse the command line arguments and returns parsed object
def parse_args():
    options = []
    return options


# This is the main orchestrator function for this project. See documentation on Git for help on how to
# use this code. The flow is pretty straight fwd:
#   1. Parse user input
#   2. Read the Metastore & Ranger configuration
#   3. Store the policies in a SQL DB - append, update or delete based on the current Ranger policies
#   4. Via CDC, look for changes that have happened in the last iteration
#   5. Apply the storage ACLs as appropriate.
def get_ranger_policies_hive_dbs():
    """

    :rtype: object
    """
    # Parse the arguments - DUMMY for the time being
    options = parse_args()
    #logging.debug(options)

    # Setup logging
    #logging.basicConfig(level=logging.debug, format='%(relativeCreated)6d %(threadName)s %(message)s')

    serverlist = os.environ["hdiclusters"]
    logging.debug("List of HDI clusters found in the config: "+serverlist)
    all_policies = pd.DataFrame()
    for aservername in serverlist.split(","):
      aserver = aservername + "-int.azurehdinsight.net"
      logging.debug("Connecting to ranger server: "+aserver)
      # Connect to Ranger and fetch the Hive policy details in a list
      ranger_hive_policies = fetch_ranger_hive_dbs(options,aserver)
      #logging.debug(str(ranger_hive_policies)) 
  
      # Now connect to Hive and fetch the database metadata details in a list
      fetch_hive_dbs(ranger_hive_policies,aserver )

      pd.set_option("display.max_columns", None)
      ranger_pdf = pd.DataFrame([x.as_dict() for x in ranger_hive_policies])
      all_policies = all_policies.append(ranger_pdf) 

    #print(tabulate(all_policies, headers='keys', tablefmt='psql'))

    return all_policies


get_ranger_policies_hive_dbs()