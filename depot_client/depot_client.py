import os

import requests
from jproperties import Properties

from logger.logger import log
from utils.constants import DEPOT_SERVICE_URL, DATAOS_RUN_AS_APIKEY, DATAOS_SECRET_DIR
from utils.utils import get_env_or_throw, get_env_or_default
from exceptions.data_os_exceptions import DataOSException


class DepotClient:
    def __init__(self):
        self.apikey = get_env_or_throw(DATAOS_RUN_AS_APIKEY)
        self.depot_service_url = get_env_or_throw(DEPOT_SERVICE_URL)

    def resolve(self, address):
        endpoint = "api/v3/resolve"
        url = f"{self.depot_service_url}/{endpoint}?address={address}"
        payload = {}
        headers = {
            'apikey': self.apikey,
        }
        response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=False)
        response.raise_for_status()
        return response.json()

    def get_depot(self, depot):
        endpoint = "api/v2/depots"
        url = f"{self.depot_service_url}/{endpoint}/{depot}"
        payload = ""
        headers = {
            'apikey': self.apikey
        }
        response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=False)
        response.raise_for_status()
        return response.json()


def get_depot_secret(depot, acl='r'):
    secret_dir_path = get_env_or_default(DATAOS_SECRET_DIR, "/etc/dataos/secret")

    secret_file_path = os.path.join(secret_dir_path, depot)
    try:
        if os.path.exists(secret_file_path):
            configs = Properties()
            with open(secret_file_path, 'rb') as config_file:
                configs.load(config_file)
            properties = configs.properties
            return properties
        elif os.environ.get(depot, None):
            config_file = os.environ.get(depot)
            configs = Properties()
            configs.load(config_file)
            properties = configs.properties
            return properties
        else:
            if not os.path.exists(secret_file_path):
                log.error(f"secret file not found for depot {depot}, path={secret_dir_path}")
                raise DataOSException(f"secret file not found for depot {depot}, path={secret_dir_path}")
            elif not os.environ.get(depot, None):
                log.error(f"secret env not found for depot {depot}")
                raise DataOSException(f"secret env not found for depot {depot}")
            else:
                log.error(f"secret not found for depot {depot}")
                raise DataOSException(f"secret not found for depot {depot}")

    except Exception as e:
        log.debug(e)
        raise DataOSException("could not find secrets", e)