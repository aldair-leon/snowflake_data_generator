import os
from azure.common import AzureHttpError
from azure.storage.blob import BlockBlobService, BlobPermissions
from scripts.env_config import read_env_file, data_folder
from scripts.init_logger import log

# Logger
logger = log('SAS TOKEN BLOB STORAGE')


def azure_blob_storage_sas_toke(blob_container):
    blob_containers = read_env_file()
    sas_container = blob_containers['blob_storage'][blob_container][0]['sas_container']
    sas_token = blob_containers['blob_storage'][blob_container][0]['sas_token']
    account_name = blob_containers['blob_storage'][blob_container][0]['account_name']
    block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)
    logger.info('<-- Trying to Establish connection SAS Token -->')
    return sas_container, block_blob_service


def azure_blob_list_file(blob_container='DEV_PSR', folder_name="processing"):
    folder_name = "/" + folder_name
    sas_token = azure_blob_storage_sas_toke(blob_container)
    sas_container = sas_token[0]
    block_blob_service = sas_token[1]
    try:
        blob_list = block_blob_service.list_blobs(sas_container, prefix=folder_name)
        for blob in blob_list:
            print(blob.name)
    except AzureHttpError as e:
        logger.error('<-- AuthenticationErrorDetail -->')


def azure_blob_upload_files(blob_container='DEV_PSR'):
    sas_token = azure_blob_storage_sas_toke(blob_container)
    sas_container = sas_token[0]
    block_blob_service = sas_token[1]
    data_folder_path = data_folder()
    for files in os.listdir(data_folder_path):
        block_blob_service.create_blob_from_path(container_name=sas_container,
                                                 blob_name='ingress/' + files,
                                                 file_path=os.path.join(data_folder_path, files))
    logger.inf('<-- Upload file finished -->')

