#
# # connection = hive.Connection(host='localhost', port=443, auth='NOSASL')
# # cursor = hive.connect('').cursor()
#
# conn = hive.connect(host='localhost', port=10001, auth='NOSASL')
# # cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
# conn.execute("SHOW DATABASES")
# status = cursor.poll().operationState
#
# while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
#     logs = cursor.fetch_logs()
#     for message in logs:
#         print(message)
#
#     # If needed, an asynchronous query can be cancelled at any time with:
#     # cursor.cancel()
#     status = cursor.poll().operationState
#
# print(cursor.fetchall())

from pyhive import hive
import pathlib, json
from TCLIService.ttypes import TOperationState

host_name = "localhost"
port = 10001
user = "admin"
password = "Qwe12rty!!"
database = "default"

def read_config():
    proj_home_abs_path = pathlib.Path(__file__).parent.parent.absolute()
    conf_file_path = proj_home_abs_path + "conf/metastore.conf"
    with open('conf_file_path') as json_file:
        data = json.load(json_file)
        return data


def hive_connection(hostname, conn_port, username, passwd, db):
    conn = hive.Connection(host=hostname, port=conn_port, username=username, password=passwd,
                           database=db, auth='CUSTOM')
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    result = cur.fetchall()
    return result


conf = read_config()
print(conf)
output = hive_connection(host_name, port, user, password, database)
print(output)
print("Done!")
