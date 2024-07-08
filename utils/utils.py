import os
import re
import yaml

from logger.logger import log
from exceptions.data_os_exceptions import InvalidConfigException
from utils.constants import CONFIG_FILE_PATH, CONFIG_FILE_PATH_DEFAULT


def get_env_or_throw(env_name):
    """Get the value of an environment variable or raise an error."""
    value = os.getenv(env_name)
    if value is None:
        log.error(f"The environment variable '{env_name}' is not set.")
        raise InvalidConfigException(f"The environment variable '{env_name}' is not set.")
    return value


def get_env_or_default(env_name, default):
    """Get the value of an environment variable or raise an error."""
    value = os.getenv(env_name)
    if value is None:
        log.warn(f"The environment variable '{env_name}' is not set.")
        return default
    return value


def stack_spec_file():
    file_path = get_env_or_default(CONFIG_FILE_PATH, CONFIG_FILE_PATH_DEFAULT)
    if not os.path.exists(file_path):
        log.error(f"Configuration file path not found: {file_path}")
        raise InvalidConfigException(f"Configuration file {file_path} not found.")

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    keys = set(config.keys())
    if "datasets" not in keys:
        log.error(f"`datasets` key missing in config")
        raise InvalidConfigException(f"`dataset` key missing in config")
    if "sqls" not in keys:
        log.warn(f"`sqls` missing in config, no tables will be created")
    return config
