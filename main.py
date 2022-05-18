from snowflake_connection import snowflake_query_execution_verify_env, snowflake_query_ctrd_tables

entity = 'ITEM'

# Verify Snowflake env
"""
Number of parameters = 1 
env = DEV_PSR (Set as default )
env available resources/env.json
"""
snowflake_query_execution_verify_env()

# Query CRTD Tables
"""
Number of parameters = 2 
env = DEV_PSR (Set as default )
entity = ITEM, ITEMLOCATION .... etc
"""
snowflake_query_ctrd_tables(entity)
