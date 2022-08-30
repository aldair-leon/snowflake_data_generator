"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.azure_blob_storage import *
from scripts.file_generation import *
from faker import Faker



def one_entity_only():
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

    number_of_records = 24000000
    number_of_files = 1
    number_of_error_records = 0

    # note inventory transaction & inventory_on_hand will generate the above number of records
    # and files for each day in the range below
    transactional_records_start = datetime.strptime('2022-07-14', '%Y-%m-%d')
    transactional_records_end = datetime.strptime('2022-07-15', '%Y-%m-%d')

    data_generation_create_data_main(entity, number_of_records, number_of_files, number_of_error_records,
                                     transactional_records_start, transactional_records_end)
    #azure_blob_upload_files(blob_container=env_azure, entity=entity)


def daily_transaction_loop():
    fake = Faker(["en_US"])

    env = 'DEV_PSR_ACCOUNT'
    env_azure = 'DEV_PSR'
    folder = 'processing'

    # Entities set inside the daily loop below
    # entity = 'inventoryonhand'  # items, locations, itemlocations, inventoryonhand, inventorytransactions,
    # itemhierarchylevelmembers, measurements

    number_of_records = 2000000
    number_of_files = 1  # default
    number_of_error_records = 0

    # Create a loop between start_date and stop_date and run transactions on a daily basis.
    # For each day, we randomize the number of files generated between low_file_count and high_file_count to mimic
    # high volume and low volume days.
    # On Sundays, we also send some updated items, locations, and item_location records to mimic master data updates
    # on a weekly basis

    start_date = datetime.strptime('2019-10-01', '%Y-%m-%d')
    end_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
    low_file_count = 1
    high_file_count = 3

    for date in [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)]:
        logger.info('Processing transactions for {0}'.format(date))
        number_of_files = fake.random_int(min=low_file_count, max=high_file_count)
        transactional_records_start = date
        transactional_records_end = date + timedelta(days=1)

        if date.weekday() == 6:
            logger.info("It's a Sunday. Adding some master data updates to the mix")
            data_generation_create_data_main("items", 50, 1,
                                             number_of_error_records, transactional_records_start,
                                             transactional_records_end)
            azure_blob_upload_files(blob_container=env_azure, entity="items")

            data_generation_create_data_main("locations", 50, 1,
                                             number_of_error_records, transactional_records_start,
                                             transactional_records_end)
            azure_blob_upload_files(blob_container=env_azure, entity="locations")

            data_generation_create_data_main("itemlocations", 2000, 1,
                                             number_of_error_records, transactional_records_start,
                                             transactional_records_end)
            azure_blob_upload_files(blob_container=env_azure, entity="itemlocations")


        data_generation_create_data_main("inventorytransactions", number_of_records, number_of_files,
                                         number_of_error_records, transactional_records_start,
                                         transactional_records_end)
        data_generation_create_data_main("inventoryonhand", number_of_records, number_of_files,
                                         number_of_error_records, transactional_records_start,
                                         transactional_records_end)
        azure_blob_upload_files(blob_container=env_azure, entity="inventorytransactions")
        azure_blob_upload_files(blob_container=env_azure, entity="inventoryonhand")


daily_transaction_loop()
# one_entity_only()