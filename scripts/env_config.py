"""
Author: Aldair Leon
Date: May 17th, 2022
"""

import json
import os
from scripts.init_logger import log

# Logger
logger = log('ENV SETUP')


# Resources folder
def env_folder_path() -> str:
    verify_path = os.path.exists(os.path.abspath("../snowflake/resources"))
    # verify_path = os.path.exists(os.path.abspath("../resources"))
    if verify_path:
        logger.info('Verify env file...')
    else:
        logger.error('Error env file, please verify your resource file!')
    return os.path.abspath("../snowflake/resources")
    # return os.path.abspath("../resources")


# Env json file
def read_env_file() -> json:
    folder_path = env_folder_path()
    verify_path = os.path.exists(os.path.abspath(folder_path + '/env.json'))
    if verify_path:
        with open(folder_path + '/env.json') as f:
            env = json.load(f)
        logger.info('Loading Snowflake credentials ...')
        return env

    else:
        logger.error('Error Loading Snowflake credentials!')


# Load json query file
def read_query_file() -> json:
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
    verify_path = os.path.exists(os.path.abspath("../snowflake/data"))
    # verify_path = os.path.exists(os.path.abspath("../data"))
    if verify_path:
        logger.info('Verify data folder...')
    else:
        logger.error('Error data folder doesnt exist, please verify your path!')
    return os.path.abspath("../snowflake/data")
    # return os.path.abspath("../data")
