"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.azure_blob_storage import *
from scripts.file_generation import *

# # Verify env
# """
# Number of parameters = 1
# env = DEV_PSR (Set as default )
# env available resources/env.json
# """
# snowflake_query_verify_env(env)
#
# # Query CRTD Tables
# """
# Number of parameters = 2
# env = DEV_PSR (Set as default )
# entity = ITEM, ITEMLOCATION .... etc
# """
# snowflake_query_ctrd_tables(entity)
#
# # Access Blob Storage list files
# """
# Number of parameters = 2
# env = DEV_PSR (Set as default )
# folder = processing, egress, summary .... etc
# """
# azure_blob_list_file(env, folder)
#
# # Access Blob Storage upload files
# """
# Number of parameters = 1
# env = DEV_PSR (Set as default )
# """
entity = 'itemlocations'  # items, locations, itemlocations, inventoryonhand, inventorytransactions,
# itemhierarchylevelmembers, measurements

env = 'DEV_PSR_ACCOUNT'
env_azure = 'DEV_PSR'
folder = 'processing'

number_of_records = 10
number_of_files = 1
number_of_error_records = 5

data_generation_create_data_main(entity, number_of_records, number_of_files, number_of_error_records)
# azure_blob_upload_files(blob_container=env_azure, entity=entity)
