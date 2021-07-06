import json
from azure.storage.filedatalake import DataLakeServiceClient

def entitiesToRemove(beforelist, afterlist):
    print("beforelist: "+ str(beforelist)+"afterlist"+str(afterlist))
    print("beforelisttype: " + str(type(beforelist)))
    return (list(set(beforelist) - set(afterlist)))   

def dlconnect():
        global service_client
        service = DataLakeServiceClient(account_url=f"https://dentsustdrngidefdrngeuw.dfs.core.windows.net",
                                        credential='bHOlAoUS+tjrIB7km3ObbTVdjGWcXfUmyy0sdCHXovst+EZvk1wU2q6oPd1WhQq9TABQTuKBHrhay1vdroeabA==')

        file_system = service.get_file_system_client('sandbox')

        #if any(i.name == self.config.DESTINATION_DIRECTORY for i in file_system.get_paths()):
        #    file_system.delete_directory(self.config.DESTINATION_DIRECTORY)

        #file_system.create_directory(self.config.DESTINATION_DIRECTORY)

        #paths = file_system.get_paths()
        #i = 0
        #for path in paths:
          #if i < 6:
            #print(path.name + '\n')

        p = 'nottrue'
        if p in ('enabled','true'):
            print('its true')
        else: 
            print('not true')
        myResources="['abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/analytics_customer_journeys.db', 'abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/hive/analytics_flame.db', 'abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/bau_automation.db', 'abfs://ar45-hive-spark-2021-06-25t16-40-42-084z@hdiprimaryajithr.dfs.core.windows.net/hive/warehouse/managed', 'abfs://ar45-hive-spark-2021-06-25t16-40-42-084z@hdiprimaryajithr.dfs.core.windows.net/hive/warehouse/managed/information_schema.db', 'abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/hive/warehouse/managed/prod_centrica.db', 'abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/prod_msft_1.db', 'abfs://ar12-spark-esp-2021-06-25t16-36-38-917z@hdiprimaryajithr.dfs.core.windows.net/prod_msft_2.db', 'abfs://ar45-hive-spark-2021-06-25t16-40-42-084z@hdiprimaryajithr.dfs.core.windows.net/hive/warehouse/managed/sys.db']"
        myotherres="path=[/apps/hive/warehouse/amcm_test_history_capture.db, /apps/hive/warehouse/amcm_test_landing_area.db, /apps/hive/warehouse/amcm_test_landing_delta_area.db, /apps/hive/warehouse/amcm_test_landing_exception_area.db, /apps/hive/warehouse/amcm_test_landing_initial_area.db, /apps/hive/warehouse/amcm_test_open_area.db, /apps/hive/warehouse/amcm_test_staging_area.db, /apps/hive/warehouse/amcm_testing.db, /apps/hive/warehouse/amcm_test_closed_area.db, /apps/hive/warehouse/amcm_test_errors_area.db]"
        hdfsentries = myResources.strip("path=[").strip("[").strip("]").split(",")
        for hdfsentry in hdfsentries:
          print(hdfsentry.strip().strip("'"))

dlconnect()

