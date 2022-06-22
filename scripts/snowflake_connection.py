"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

import snowflake.connector
from scripts.env_config import read_env_file, read_query_file
from snowflake.connector.errors import DatabaseError, ProgrammingError
from scripts.init_logger import log
from scripts.list_file_processing import processing_folder_list

# Logger
logger = log('SNOWFLAKE CTX')


# Establish connection between python - snowflake
def snowflake_connection(snowflake_env: str) -> snowflake.connector.connection:
    """

            Establish connection between python and snowflake, then return connection string.

            :param snowflake_env: -> str
            :return: ctx -> Snowflake connection

    """
    env_cred = read_env_file()
    password = env_cred['snowflake'][snowflake_env][0]["password"]
    user = env_cred['snowflake'][snowflake_env][0]["user"]
    account = env_cred['snowflake'][snowflake_env][0]["account"]
    warehouse = env_cred['snowflake'][snowflake_env][0]["warehouse"]
    database = env_cred['snowflake'][snowflake_env][0]["database"]
    schema = env_cred['snowflake'][snowflake_env][0]["schema"]
    role = env_cred['snowflake'][snowflake_env][0]["role"]
    try:
        ctx = snowflake.connector.connect(
            password=password,
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
def snowflake_query_verify_env(env: str = 'DEV_PSR_ACCOUNT'):
    """

                Execute query to verify env

                :param env: -> str

    """
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
def snowflake_query_ctrd_tables(entity: str = '', query_name: str = 'query_crtd_table_entity',
                                env: str = 'DEV_PSR_ACCOUNT', number_of_records: str = '10'):
    """

                Execute query into CRTD tables depending on which entity you provide.
                Queries are define in resources/query.json

                :param len_item_list:
                :param number_of_records:
                :param query_name:
                :param entity: -> str
                :param env: -> str
                :return: result -> pandas Data frame

    """
    query_file = read_query_file()
    ctx = snowflake_connection(env)
    cursor = ctx.cursor()
    if query_name != 'query_crtd_table_item_locations':
        query_crtd_entity = query_file[query_name].format('HIERARCHYLEVELIDENTIFIER', entity, number_of_records)
        try:
            logger.info('Executing query....{0}'.format(query_crtd_entity))
            execution = cursor.execute(query_crtd_entity)
            # result = execution.fetch_pandas_all()
            result = execution.fetch_pandas_all()
            return result

        except ProgrammingError as e:
            logger.error(e)
            logger.error('<-- Query syntax error -->')
            logger.error('Verify your query: {0}'.format(query_crtd_entity))

    else:
        # item_list_format = ",".join(['%s'] * len(item_list))
        query_crtd_entity = query_file[query_name].format(number_of_records)

        try:
            logger.info('Executing query....{0}'.format(query_crtd_entity))
            # execution = cursor.execute(query_crtd_entity, (item_list))
            execution = cursor.execute(query_crtd_entity)
            result = execution.fetch_pandas_all()
            return result

        except ProgrammingError as e:
            logger.error(e)
            logger.error('<-- Query syntax error -->')
            logger.error('Verify your query: {0}'.format(query_crtd_entity))


# Query STATS Table
def snowflake_query_stats_table(query_name: str = 'query_ingestion_time',
                                env: str = 'DEV_PSR_ACCOUNT', entity: str = 'items'):
    """


    """
    query_file = read_query_file()
    ctx = snowflake_connection(env)
    cursor = ctx.cursor()
    files = processing_folder_list(entity)
    file_name_list = ",".join(['%s'] * len(files))

    try:
        query_stats = query_file[query_name].format(file_name_list)
        logger.info('Executing query....{0}'.format(query_stats))
        execution = cursor.execute(query_stats, files)
        result = execution.fetch_pandas_all()
        return result

    except ProgrammingError as e:
        logger.error(e)
        logger.error('<-- Query syntax error -->')
        logger.error('Verify your query: {0}'.format(query_file))


# Query STATS Table
def snowflake_query_last_ingestion(query_name: str = 'query_last_ingestion',
                                   env: str = 'DEV_PSR_ACCOUNT'):
    """


    """
    query_file = read_query_file()
    ctx = snowflake_connection(env)
    cursor = ctx.cursor()

    try:
        query_stats = query_file[query_name]
        logger.info('Executing query....{0}'.format(query_stats))
        execution = cursor.execute(query_stats)
        result = execution.fetch_pandas_all()
        return result

    except ProgrammingError as e:
        logger.error(e)
        logger.error('<-- Query syntax error -->')
        logger.error('Verify your query: {0}'.format(query_file))
