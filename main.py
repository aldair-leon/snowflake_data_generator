"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.azure_blob_storage import azure_blob_storage_sas_toke, azure_blob_upload_files, azure_blob_list_file
from scripts.snowflake_connection import snowflake_query_verify_env, snowflake_query_ctrd_tables
from scripts.data_generation import data_generation_create_data_main, data_item, data_item_locations, \
    data_inventory_on_hand

entity = 'inventoryonhand'  # items, locations, itemlocations, inventoryonhand, inventorytransactions,
# itemhierarchylevelmembers, measurements
env = 'DEV_PSR'
folder = 'processing'

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

# data_generation_create_data_main(entity, 1000, 2)
azure_blob_upload_files(blob_container=env, entity=entity)
