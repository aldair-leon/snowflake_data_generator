# Read Files Blob storage
from azure.storage.blob import BlockBlobService, BlobPermissions
import os

sas_container = '6d99e867-063d-4996-98ca-9830d6ccf3d4'
sas_token = 'sp=racwl&st=2022-04-29T16:57:38Z&se=2022-07-30T00:57:38Z&spr=https&sv=2020-08-04&sr=c&sig=26MDHh05HMz6FAXpO8s8im5WhTZyXuRP%2BK3nGuuaizw%3D'
file_path = 'C://Users//1027147//PycharmProjects//snwoflake//data//items_ISDM-2021.2.0_20220528T043900Z_Test001.csv'
blob_service = BlockBlobService(account_name='storjdpblkprdeus201', sas_token=sas_token)
blob_service.create_blob_from_path(container_name=sas_container,blob_name='ingress/items_ISDM-2021.2.0_20220528T043900Z_Test001.csv',
                                   file_path=file_path)



# blob_list = blob_service.list_blobs(sas_container, prefix="processing/")
# for blob in blob_list:
#     print(blob.name)

