import json
import logging
import pathlib

import requests
from requests.auth import HTTPBasicAuth


# Ranger policy class
class RangerPolicy:
    def __init__(self, policy_id, create_date, update_date, policy_name, resource_name, repository_name,
                 repository_type, user_list, group_list, perm_list, databases, tables, columns, is_enabled,
                 is_recursive):
        self._policy_id = policy_id
        self._create_date = create_date
        self._update_date = update_date
        self._policy_name = policy_name
        self._resource_name = resource_name
        self._repository_name = repository_name
        self._repository_type = repository_type
        self._user_list = user_list
        self._group_list = group_list
        self._perm_list = perm_list
        self._databases = databases
        self._tables = tables
        self._columns = columns
        self._is_enabled = is_enabled
        self._is_recursive = is_recursive

    @property
    def policy_id(self):
        return self._policy_id

    @policy_id.setter
    def policy_id(self, value):
        self._policy_id = value

    @property
    def create_date(self):
        return self._create_date

    @create_date.setter
    def create_date(self, value):
        self._create_date = value

    @property
    def update_date(self):
        return self._update_date

    @update_date.setter
    def update_date(self, value):
        self._update_date = value

    @property
    def policy_name(self):
        return self._policy_name

    @policy_name.setter
    def policy_name(self, value):
        self._policy_name = value

    @property
    def resource_name(self):
        return self._resource_name

    @resource_name.setter
    def resource_name(self, value):
        self._resource_name = value

    @property
    def repository_type(self):
        return self._repository_type

    @repository_type.setter
    def repository_type(self, value):
        self._repository_type = value

    @property
    def user_list(self):
        return self._user_list

    @user_list.setter
    def user_list(self, value):
        self._user_list = value

    @property
    def group_list(self):
        return self._group_list

    @group_list.setter
    def group_list(self, value):
        self._group_list = value

    @property
    def perm_list(self):
        return self._perm_list

    @perm_list.setter
    def perm_list(self, value):
        self._perm_list = value

    @property
    def databases(self):
        return self._databases

    @databases.setter
    def databases(self, value):
        self._databases = value

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, value):
        self._tables = value

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = value

    @property
    def is_enabled(self):
        return self._is_enabled

    @is_enabled.setter
    def is_enabled(self, value):
        self._is_enabled = value

    @property
    def is_recursive(self):
        return self._is_recursive

    @is_recursive.setter
    def is_recursive(self, value):
        self._is_recursive = value


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

    if json_formatted_policies["totalCount"] > json_formatted_policies["pageSize"]:
        # Handle the pagination case here. Not sure if pagination is indeed present or not
        pass

    all_ranger_hive_policies = []
    for policy in json_formatted_policies["vXPolicies"]:
        if policy["repositoryType"].lower() == "hive":
            ranger_policy = RangerPolicy(policy["id"], policy["createDate"], policy["updateDate"], policy["policyName"],
                                         policy["policyName"], policy["resourceName"], policy["repositoryName"],
                                         policy["repositoryType"], policy["userList"], policy["groupList"],
                                         policy["permList"], policy["databases"], policy["tables"], policy["columns"],
                                         policy["isEnabled"], policy["isRecursive"])
            all_ranger_hive_policies += ranger_policy
        else:
            logging.info("Not a Hive policy: " + ranger_policy["policyName"] + ". Continuing...")
            continue

    # For each Hive DB from the metadata list, let us populate the corresponding Ranger policy
    for db_meta in hive_db_meta_list:
        pass
