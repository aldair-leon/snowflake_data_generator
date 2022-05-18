from scripts.azure_blob_storage import azure_blob_storage_sas_toke
from scripts.snowflake_connection import snowflake_query_verify_env, snowflake_query_ctrd_tables

entity = 'ITEM'
env = 'JCP_PSR'

# Verify Snowflake env
"""
Number of parameters = 1 
env = DEV_PSR (Set as default )
env available resources/env.json
"""
#snowflake_query_verify_env(env)

# Query CRTD Tables
"""
Number of parameters = 2 
env = DEV_PSR (Set as default )
entity = ITEM, ITEMLOCATION .... etc
"""
#snowflake_query_ctrd_tables(entity)


# Access Blob Storage and list files
"""
Number of parameters = 2 
env = DEV_PSR (Set as default )
folder = processing, egress, summary .... etc
"""
azure_blob_storage_sas_toke('JCP_PSR')
