from pyhive import hive
from TCLIService.ttypes import TOperationState

cursor = hive.connect('localhost').cursor()
# cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
cursor.execute("SHOW DATABASES")
status = cursor.poll().operationState

while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    logs = cursor.fetch_logs()
    for message in logs:
        print(message)

    # If needed, an asynchronous query can be cancelled at any time with:
    # cursor.cancel()
    status = cursor.poll().operationState

print(cursor.fetchall())
