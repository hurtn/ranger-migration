from hiveMetastore.hive_ms import fetch_hive_dbs, read_ms_conf
from storePolicies import storePolicies


# Parse the command line arguments and returns parsed object
def parse_args():
    options = []
    return options


#
# This is the main orchestrator function for this project. See documentation on Git for help on how to
# use this code. The flow is pretty straight fwd:
#   1. Parse user input
#   2. Read the Metastore & Ranger configuration
#   3. Store the policies in a SQL DB - append, update or delete based on the current Ranger policies
#   4. Via CDC, look for changes that have happened in the last iteration
#   5. Apply the storage ACLs as appropriate.
#
def main():
    # Parse the arguments
    options = parse_args()
    print(options)

    # Connect to Hive and fetch the database details in a list
    list_of_dbs = fetch_hive_dbs(options)
    print(list_of_dbs)

    # Connect to Ranger and fetch the Hive policy details in a list
    ranger_hive_policies = fetch_ranger_hive_policies(options)
    print(ranger_hive_policies)

    # Store the policies in SQL DB
    store_policies(options)

# The ever important dummy main function
if __name__ == "__main__":
    main()
