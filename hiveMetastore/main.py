import logging

import numpy as np
import pandas as pd
from tabulate import tabulate

from hive_ms import fetch_hive_dbs
from ranger import fetch_ranger_hive_dbs


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
    logging.debug(options)

    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')

    # Connect to Ranger and fetch the Hive policy details in a list
    ranger_hive_policies = fetch_ranger_hive_dbs(options)
    logging.debug(str(ranger_hive_policies))

    # Now connect to Hive and fetch the database metadata details in a list
    fetch_hive_dbs(ranger_hive_policies)

    pd.set_option("display.max_columns", None)
    ranger_pdf = pd.DataFrame([x.as_dict() for x in ranger_hive_policies])

    print(tabulate(ranger_pdf, headers='keys', tablefmt='psql'))

    return ranger_pdf
