#
# This file contains functions to retrieve credentials for various Azure services that are used in the project.
# The service principal that is used to run this code will have to be granted the appropriate permissions on
# Azure Key Vault for these functions to work fine. Else, they will throw an appropriate security exception.
#

# Get Azure Key Vault entry for Hive metastore passwd
def get_ms_credentials(ms_key):
    pass


# Get Azure Key Vault entry for Ranger
def get_ranger_credentials(ranger_key):
    pass


# Get Azure Key Vault entry for storage account
def get_storage_credentials(storage_account_key):
    pass
