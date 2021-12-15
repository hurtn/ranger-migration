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

import json
import logging
import pathlib
import os
import requests
from requests.auth import HTTPBasicAuth
from collections import defaultdict


# Ranger policy class
class RangerPolicy:
    def __init__(self, policy_id, policy_name, repository_name, repository_type, perm_map_list, databases, is_enabled,
                 is_recursive, tables, table_type):
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
        self.tables = tables
        self.tbl_names =  defaultdict(str)
        self.table_type = table_type

    def as_dict(self):
        return {'policy_id': self.policy_id, 'policy_name': self.policy_name, 'repository_name': self.repository_name,
                'repository_type': self.repository_type, 'perm_map_list': self.perm_map_list,
                'databases': self.databases, 'is_enabled': self.is_enabled, 'is_recursive': self.is_recursive,
                'paths': self.paths, 'db_names': self.db_names, 'tables': self.tables, 'table_type': self.table_type, 'tbl_names': json.dumps(self.tbl_names)}

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
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, value):
        self._tables = value

    @property
    def table_type(self):
        return self._table_type

    @table_type.setter
    def table_type(self, value):
        self._table_type = value
                
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

    def set_hive_tbl_names(self, tbl_name,tbl_path):
        self.tbl_names[tbl_name]=tbl_path


def fetch_ranger_hive_dbs(endpoint):
    logging.info("ranger.py: Fetching policies from ranger endpoint: " + endpoint)
    startIndex=0
    fetchPolicies = True
    all_ranger_hive_policies = []
    allpolicies = {}
    counter =0 
    while fetchPolicies:
        r = requests.get(endpoint+'&pageSize=' + str(os.environ.get("rangerPageSize",100)) + '&startIndex='+str(startIndex), auth=HTTPBasicAuth(os.environ["rangerusername"], os.environ["rangerpassword"]))
        if r.status_code==200:
            policies = r.json()
            #logging.info("Policie:" + json.dumpsjson_formatted_policies))
            #logging.info("Result size " + str(policies["resultSize"]))
            counter +=1
            if policies["resultSize"]== 0: 
                fetchPolicies = False
            else:
                startIndex += 100
               
                #logging.info("Policies array has " + str(len(policies["vXPolicies"])))
                for policy in policies["vXPolicies"]:
                    if (policy["repositoryType"].lower() == "hive") and ("databases" in policy) and ("tables" in policy) and '' != policy["databases"]:
                        # Now we are all set to create the RangerPolicy object
                        #logging.info('Capturing policy ID '+ str(policy["id"]))
                        ranger_policy = RangerPolicy(policy["id"], policy["policyName"], policy["repositoryName"],
                                                    policy["repositoryType"], policy["permMapList"], policy["databases"],
                                                    policy["isEnabled"], policy["isRecursive"], policy["tables"], policy["tableType"])
                        all_ranger_hive_policies.append(ranger_policy)
                    else:
                        logging.debug("Ignoring non hive policy: " + policy["policyName"] + ". Continuing...")
                        continue
    #logging.info(str(all_ranger_hive_policies))
    return all_ranger_hive_policies

