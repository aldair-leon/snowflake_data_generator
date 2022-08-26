import os
from scripts.env_config import data_folder
from scripts.init_logger import log

# Logger
logger = log('LIST FILE')


def processing_folder_list(entity):
    """
        List all the files to be processing from local to blob storage

    :param entity:
    :return: file_name
    """
    data_folder_path = data_folder()
    data_folder_path_ingress = os.path.join(data_folder_path, 'processing', entity)
    file_name = []
    for files in os.listdir(data_folder_path_ingress):
        file_name.append(files)
    logger.info('READING FOLDER....')
    return file_name

