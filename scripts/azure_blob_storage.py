"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

import os
import shutil
from scripts.data_generation import data_folder_ingress_processing
from azure.common import AzureHttpError
from azure.storage.blob import BlockBlobService
from scripts.env_config import read_env_file, data_folder
from scripts.init_logger import log

# Logger
logger = log('SAS TOKEN BLOB STORAGE')


def azure_blob_storage_sas_toke(blob_container):
    """

                This function establish connection between python and blob container using sas_token.

                :param: blob_container -> str
                :return: sas_container -> str
                :return: block_blob_service -> SAS Token

    """
    blob_containers = read_env_file()
    sas_container = blob_containers['blob_storage'][blob_container][0]['sas_container']
    sas_token = blob_containers['blob_storage'][blob_container][0]['sas_token']
    account_name = blob_containers['blob_storage'][blob_container][0]['account_name']
    block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)
    logger.info('<-- Trying to Establish connection SAS Token -->')
    return sas_container, block_blob_service


def azure_blob_list_file(blob_container: str = 'DEV_PSR', folder_name: str = "processing"):
    """

                This function create a list with all the paths that contain an specific folder_name.

                :param blob_container -> str
                :param folder_name -> str
                :return: blob_paths -> list

    """
    blob_paths = []
    folder_name = os.path.join("/", folder_name)
    sas_token = azure_blob_storage_sas_toke(blob_container)
    sas_container = sas_token[0]
    block_blob_service = sas_token[1]
    try:
        blob_list = block_blob_service.list_blobs(sas_container, prefix=folder_name)
        for blob in blob_list:
            blob_paths.append(blob.name)
        return blob_paths
    except AzureHttpError as e:
        logger.error('<-- AuthenticationErrorDetail -->')
        logger.error(e)


def azure_blob_upload_files(blob_container: str = 'DEV_PSR', blob_name: str = 'ingress', entity: str = ''):
    """

                This function upload files in a specific blob folder.

                :param entity:
                :param blob_container -> str
                :param blob_name -> str

    """
    data_folder_ingress_processing()
    sas_token = azure_blob_storage_sas_toke(blob_container)
    sas_container = sas_token[0]
    block_blob_service = sas_token[1]
    data_folder_path = data_folder()
    data_folder_path_ingress = os.path.join(data_folder_path, 'ingress', entity)
    data_folder_path_processing = os.path.join(data_folder_path, 'processing', entity)
    blob_path = azure_blob_list_file(blob_container, blob_name)
    for files in os.listdir(data_folder_path_ingress):
        if blob_name in blob_path:
            blob_name_path = os.path.join(blob_name, files)
            file_path = os.path.join(data_folder_path_ingress, files)
            block_blob_service.create_blob_from_path(container_name=sas_container,
                                                     blob_name=blob_name_path,
                                                     file_path=file_path)
            shutil.move(file_path, data_folder_path_processing)

            logger.info('<-- Upload file finished -->')
        else:
            logger.error('<-- Path doesnt exist -->')
