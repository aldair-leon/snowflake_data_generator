"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.azure_blob_storage import *
from scripts.snowflake_connection import *
from scripts.data_generation import *
from scripts.list_file_processing import *

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
entity = 'locations'  # items, locations, itemlocations, inventoryonhand, inventorytransactions,
# itemhierarchylevelmembers, measurements
env = 'DEV_PSR_ACCOUNT'
folder = 'processing'
# data_generation_create_data_main(entity, 100000, 200)
# azure_blob_upload_files(blob_container=env, entity=entity)

#snowflake_query_stats_table(entity='locations')
# print(processing_folder_list('items'))
