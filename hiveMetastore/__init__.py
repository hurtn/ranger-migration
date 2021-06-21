import logging
from datetime import datetime
import azure.functions as func

from hiveMetastore.main import get_ranger_policies_hive_dbs

logging.getLogger(__name__).addHandler(logging.NullHandler())


def main(mytimer: func.TimerRequest) -> None:
    """

    :rtype: object
    """
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    get_ranger_policies_hive_dbs()
