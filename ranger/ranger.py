import json
import logging

import requests
from requests.auth import HTTPBasicAuth


def get_ranger_policies():
    endpoint = 'https://[clustername].azurehdinsight.net/ranger/service/public/api/policy/'
    # headers={"Authorization": "Basic %s" % b64Val}
    r = requests.get(endpoint, auth=HTTPBasicAuth('admin', 'Qwe12rty!!'))
    policies = r.json()
    json_formatted_policies = json.dumps(policies, indent=4)
    logging.info(json_formatted_policies)
    return json_formatted_policies


def get_ranger_hive_policies(hive_db_meta_list):
    # At first, let us get the latest Ranger policies as a dump
    json_formatted_policies = get_ranger_policies()

    # Now, let us filter only the Hive policies from this list

def populate_ranger_policy_per_db():

    policies_map = get_ranger_hive_policies()

    # For each Hive DB from the metadata list, let us populate the corresponding Ranger policy
    for db_meta in hive_db_meta_list:
        pass
