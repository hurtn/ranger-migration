import json
import logging
import pathlib

import requests
from requests.auth import HTTPBasicAuth


# Ranger policy class
class RangerPolicy:
    def __init__(self, policy_id, create_date, update_date, policy_name, resource_name, description, repository_name,
                 repository_type, user_list, group_list, perm_list, databases, is_enabled, is_recursive):
        self.policy_id = policy_id
        self.create_date = create_date
        self.update_date = update_date
        self.policy_name = policy_name
        self.resource_name = resource_name
        self.desc = description
        self.repository_name = repository_name
        self.repository_type = repository_type
        self.user_list = user_list
        self.group_list = group_list
        self.perm_list = perm_list
        self.databases = databases
        self.is_enabled = is_enabled
        self.is_recursive = is_recursive

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
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        self._desc = value

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
    conf_file_path = str(proj_home_abs_path) + "/conf/ranger_conf.json"
    with open(conf_file_path) as json_file:
        data = json.load(json_file)
    logging.info("Read the Ranger config")
    logging.debug(data)
    return data


def get_ranger_policies():
    conf = get_ranger_conf()
    logging.debug("Ranger config: " + str(conf))
    all_filters = ""
    for flt in conf["filters"]:
        key = flt.keys()
        for k in key:
            value = flt[k]
            all_filters += k + "=" + value
            all_filters += "&"
    logging.debug("All filters: " + all_filters)
    endpoint = "https://" + conf["server"] + "/" + conf["extension"] + "?" + all_filters
    logging.debug("Ranger end point: " + endpoint)
    r = requests.get(endpoint, auth=HTTPBasicAuth(conf["user_name"], conf["password"]))
    policies = r.json()
    logging.debug("Ranger policies: " + str(policies))
    return policies


def fetch_ranger_hive_dbs(options):
    # At first, let us get the latest Ranger Hive policies
    json_formatted_policies = get_ranger_policies()

    # Handle the pagination case here. Not sure if pagination is indeed present or not
    if json_formatted_policies["totalCount"] > json_formatted_policies["pageSize"]:
        pass

    all_ranger_hive_policies = []
    for policy in json_formatted_policies["vXPolicies"]:
        logging.debug("Ranger policy being handled: " + str(policy))
        if (policy["repositoryType"].lower() == "hive") and ("databases" in policy):
            # Flatten the "permMapList" list field
            users = ""
            groups = ""
            perms = ""
            for perm_map in policy["permMapList"]:
                for user in perm_map["userList"]:
                    users += user + ","
                for group in perm_map["groupList"]:
                    groups += group + ","
                for perm in perm_map["permList"]:
                    perms += perm + ","

            # Extract the database name from resource name field
            resources = policy["resourceName"].split("/")
            logging.info("Extracted database name " + resources[1])

            # Now we are all set to create the RangerPolicy object
            ranger_policy = RangerPolicy(policy["id"], policy["createDate"], policy["updateDate"], policy["policyName"],
                                         policy["policyName"], resources[1], policy["repositoryName"],
                                         policy["repositoryType"], users, groups, perms, policy["databases"],
                                         policy["isEnabled"], policy["isRecursive"])
            all_ranger_hive_policies.append(ranger_policy)
        else:
            logging.info("Ignoring policy: " + policy["policyName"] + ". Continuing...")
            continue

    return all_ranger_hive_policies
