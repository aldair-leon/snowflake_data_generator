"""
Author: Aldair Leon
Date: May 17th, 2022
"""

import snowflake.connector
from scripts.env_config import read_env_file, read_query_file
from snowflake.connector.errors import DatabaseError, ProgrammingError
from scripts.init_logger import log

# Logger
logger = log('SNOWFLAKE CTX')


# Establish connection between python - snowflake
def snowflake_connection(snowflake_env) -> snowflake.connector.connection:
    env_cred = read_env_file()
    type_auth = env_cred['snowflake'][snowflake_env][0]["authenticator"]
    user = env_cred['snowflake'][snowflake_env][0]["user"]
    account = env_cred['snowflake'][snowflake_env][0]["account"]
    warehouse = env_cred['snowflake'][snowflake_env][0]["warehouse"]
    database = env_cred['snowflake'][snowflake_env][0]["database"]
    schema = env_cred['snowflake'][snowflake_env][0]["schema"]
    role = env_cred['snowflake'][snowflake_env][0]["role"]
    try:
        ctx = snowflake.connector.connect(
            authenticator=type_auth,
            role=role,
            user=user,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            autocommit=False
        )
        logger.info("Snowflake connection complete!")
        return ctx
    except DatabaseError as db_ex:
        if db_ex.errno == 250001:
            logger.error(db_ex)
            logger.info('\n <--- Snowflake connection information ---> \n User: {0} \n Account: {1} \n Warehouse: {2} '
                        '\n Database: {3} \n Schema: {4} \n Role: {5}'.format(user, account, warehouse, database,
                                                                              schema, role))
        else:
            raise


# Verify correct snowflake env
def snowflake_query_verify_env(env='DEV_PSR'):
    query_file = read_query_file()
    query = query_file["query_profile"]
    ctx = snowflake_connection(env)
    cursor = ctx.cursor()
    try:
        logger.info('Executing query....{0}'.format(query))
        execution = cursor.execute(query)
        result = execution.fetchone()
        print(' User: {0} \n Account: {1} \n Warehouse: {2} '
              '\n Database: {3} \n Schema: {4} \n Role: {5}'.format(result[0], result[2], result[3], result[4],
                                                                    result[5], result[1]))
    except ProgrammingError as e:
        logger.error(e)
        logger.error('<-- Query syntax error -->')
        logger.error('Verify your query: {0}'.format(query))


# Query CRTD Tables
"""
Query predefine in resources/query.json
"""


def snowflake_query_ctrd_tables(entity, env='DEV_PSR'):
    query_file = read_query_file()
    query_crtd_entity = query_file["query_crtd_table_entity"].format(entity)
    # Line 76 : SELECT * FROM CRTD_{entity} LIMIT 10; --> for testing just query top 10 values
    ctx = snowflake_connection(env)
    cursor = ctx.cursor()

    try:
        logger.info('Executing query....{0}'.format(query_crtd_entity))
        execution = cursor.execute(query_crtd_entity)
        result = execution.fetch_pandas_all()
        print(result)

    except ProgrammingError as e:
        logger.error(e)
        logger.error('<-- Query syntax error -->')
        logger.error('Verify your query: {0}'.format(query_crtd_entity))
