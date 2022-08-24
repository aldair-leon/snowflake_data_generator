"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

import json
import os
from scripts.init_logger import log

# Logger
logger = log('ENV SETUP')


# Resources folder
def env_folder_path() -> str:
    """

                This function verify if snowflake/resources directory exist.

    """
    verify_path = os.path.exists(os.path.abspath("../snowflake_data_generator/resources"))
    # verify_path = os.path.exists(os.path.abspath("../resources"))
    if verify_path:
        logger.info('Verify env file...')
    else:
        logger.error('Error env file, please verify your resource file!')
    return os.path.abspath("../snowflake_data_generator/resources")
    # return os.path.abspath("../resources")


# Env json file
def read_env_file() -> json:
    """

                This function verify if /env.json file exist, load and return all the credentials

    """
    folder_path = env_folder_path()
    verify_path = os.path.exists(os.path.abspath(folder_path + '/env.json'))
    if verify_path:
        with open(folder_path + '/env.json') as f:
            env = json.load(f)
        logger.info('Loading credentials ...')
        return env

    else:
        logger.error('Error Loading credentials!')


# Load json query file
def read_query_file() -> json:
    """

                This function verify if /query.json file exist, load and return all the queries

    """
    folder_path = env_folder_path()
    verify_path = os.path.exists(os.path.abspath(folder_path + '/query.json'))
    if verify_path:
        with open(folder_path + '/query.json') as f:
            env = json.load(f)
        logger.info('Loading query file ...')
        return env

    else:
        logger.error('Error Loading query file!')


# Data folder
def data_folder() -> str:
    """

                This function verify if snowflake/data file exist and return abspath

    """
    verify_path = os.path.exists(os.path.abspath("../snowflake_data_generator/data"))
    # verify_path = os.path.exists(os.path.abspath("../data"))
    if verify_path:
        logger.info('Verify data folder...')
        return os.path.abspath("../snowflake_data_generator/data")
        # return os.path.abspath("../data")
    else:
        logger.error('Error data folder doesnt exist, please verify your path!')


# Data folder
def entity_file() -> str:
    """

                This function verify entity.json file

    """
    folder_path = env_folder_path()
    verify_path = os.path.exists(os.path.abspath(folder_path + '/entities.json'))
    if verify_path:
        with open(folder_path + '/entities.json') as f:
            entity = json.load(f)
            logger.info('Loading entity file ...')
        return entity

    else:
        logger.error('Error Loading entity file!')


# Env options

def env_options():
    env = read_env_file()
    return list(env['snowflake'].keys()), list(env['blob_storage'].keys())


def snowflake_account_blob_storage(blob_env):
    env_file = read_env_file()
    blob = list(env_file['blob_storage']).index(str(blob_env))
    snow = list(env_file['snowflake'])[int(blob)]
    return snow
