import json
import logging
import pathlib

import requests
from requests.auth import HTTPBasicAuth


# Ranger policy class
class RangerPolicy:
    def __init__(self, policy_id, policy_name, repository_name, repository_type, perm_map_list, databases, is_enabled,
                 is_recursive):
        self.policy_id = policy_id
        self.policy_name = policy_name
        self.repository_name = repository_name
        self.repository_type = repository_type
        self.perm_map_list = perm_map_list
        self.databases = databases
        self.is_enabled = is_enabled
        self.is_recursive = is_recursive
        self.db_names = []
        self.paths = []

    def as_dict(self):
        return {'policy_id': self.policy_id, 'policy_name': self.policy_name, 'repository_name': self.repository_name,
                'repository_type': self.repository_type, 'perm_map_list': self.perm_map_list,
                'databases': self.databases, 'is_enabled': self.is_enabled, 'is_recursive': self.is_recursive,
                'paths': self.paths, 'db_names': self.db_names}

    @property
    def policy_id(self):
        return self._policy_id

    @policy_id.setter
    def policy_id(self, value):
        self._policy_id = value

    @property
    def policy_name(self):
        return self._policy_name

    @policy_name.setter
    def policy_name(self, value):
        self._policy_name = value

    @property
    def repository_name(self):
        return self._repository_name

    @repository_name.setter
    def repository_name(self, value):
        self._repository_name = value

    @property
    def repository_type(self):
        return self._repository_type

    @repository_type.setter
    def repository_type(self, value):
        self._repository_type = value

    @property
    def perm_map_list(self):
        return self._perm_map_list

    @perm_map_list.setter
    def perm_map_list(self, value):
        self._perm_map_list = value

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

    def set_hive_db_paths(self, paths):
        self.paths.append(paths)

    def set_hive_db_names(self, names):
        self.db_names.append(names)


def get_ranger_conf():
    # Gets the Ranger configurations set in the conf/ranger_conf.json file.
    data = None
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = str(proj_home_abs_path) + "/conf/ranger_conf.json"
    with open(conf_file_path) as json_file:
        data = json.load(json_file)
    return data


def get_ranger_policies(servername):
    conf = get_ranger_conf()
    if servername:
      pservername = servername
    else:
      pservername = ms_conf_dict["server"]

    all_filters = ""
    for flt in conf["filters"]:
        key = flt.keys()
        for k in key:
            value = flt[k]
            all_filters += k + "=" + value
            all_filters += "&"
    endpoint = "https://" + pservername  + "/" + conf["extension"] + "?" + all_filters
    logging.debug("Fetching policies from ranger endpoint: " + endpoint)
    r = requests.get(endpoint, auth=HTTPBasicAuth(conf["user_name"], conf["password"]))
    policies = r.json()
    return policies


def fetch_ranger_hive_dbs(options,servername):
    # At first, let us get the latest Ranger Hive policies
    json_formatted_policies = get_ranger_policies(servername)

    # Handle the pagination case here. Not sure if pagination is indeed present or not
    if json_formatted_policies["totalCount"] > json_formatted_policies["pageSize"]:
        pass

    all_ranger_hive_policies = []
    for policy in json_formatted_policies["vXPolicies"]:
        if (policy["repositoryType"].lower() == "hive") and ("databases" in policy) and '' != policy["databases"]:
            # Now we are all set to create the RangerPolicy object
            ranger_policy = RangerPolicy(policy["id"], policy["policyName"], policy["repositoryName"],
                                         policy["repositoryType"], policy["permMapList"], policy["databases"],
                                         policy["isEnabled"], policy["isRecursive"])
            all_ranger_hive_policies.append(ranger_policy)
        else:
            logging.debug("Ignoring non hive policy: " + policy["policyName"] + ". Continuing...")
            continue

    return all_ranger_hive_policies

#get_ranger_policies()