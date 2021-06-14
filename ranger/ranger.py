import json
import logging
import pathlib

import requests
from requests.auth import HTTPBasicAuth


# Ranger policy class
class RangerPolicy:
    id = ""
    createDate = ""
    updateDate = ""
    policyName = ""
    resourceName = ""
    repositoryName = ""
    repositoryType = ""
    userList = []
    groupList = []
    permList = []
    databases = "default"
    tables = "*"
    columns = "*"
    columnType = "Inclusion"
    isEnabled = True
    isRecursive = False
    isAuditEnabled = True
    replacePerm = False


def get_ranger_conf():
    # Gets the Ranger configurations set in the conf/ranger_conf.json file.
    data = None
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = proj_home_abs_path + "conf/ranger_conf.json"
    with open('conf_file_path') as json_file:
        data = json.load(json_file)
    logging.info("Read the Ranger config")
    logging.debug(data)
    return data


def get_ranger_policies():
    conf = get_ranger_conf()
    all_filters = ""
    for flt in conf.filters:
        key = flt.keys()
        value = flt[key]
        all_filters += key + "=" + value
        all_filters += "&"
    endpoint = "https://" + conf.server + conf.extension + "?" + all_filters
    r = requests.get(endpoint, auth=HTTPBasicAuth(conf.username, conf.password))
    policies = r.json()
    json_formatted_policies = json.dumps(policies, indent=4)
    logging.info(json_formatted_policies)
    return json_formatted_policies


def fetch_ranger_hive_dbs(options):
    # At first, let us get the latest Ranger Hive policies
    json_formatted_policies = get_ranger_policies()

    policies_map = get_ranger_hive_policies()

    # For each Hive DB from the metadata list, let us populate the corresponding Ranger policy
    for db_meta in hive_db_meta_list:
        pass
