import pandas as pd
from scripts.data_generation import data_locations, data_itemhierarchylevelmembers, data_item, data_item_locations, \
    data_inventory_on_hand, data_inventory_transactions,data_generation_load_header_columns,data_folder_ingress_processing
import os
from scripts.init_logger import log
from datetime import datetime
from scripts.snowflake_connection import snowflake_query_ctrd_tables

# Logger
logger = log('FILE GENERATION')


def data_generation_create_file_locations(locations_df, number_files, number_records, ingress,
                                          name_file, columns_position, columns_name, file_header):
    """

        This function create csv file with data provided by data_locations(number_records)

        :param locations_df:
        :param number_files:
        :param number_records:
        :param ingress:
        :param name_file:
        :param columns_position:
        :param columns_name:
        :param file_header:
        :return:

    """
    for i in range(0, number_files):
        data = data_locations(number_records)
        join_location_file_path = os.path.join(ingress, 'locations', name_file)
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        for j in range(0, len(columns_name)):
            locations_df[file_header[columns_position[j]]] = data[j]
        logger.info(f'File no: {i + 1} of {number_files}')
        locations_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


def data_generation_create_file_item_hierarchy_level_members(item_hierarchy_level_members_df, file_header,
                                                             number_records, number_files, ingress, name_file):
    """

        This function create csv file with data provided by data_itemhierarchylevelmembers(number_records)

        :param item_hierarchy_level_members_df:
        :param file_header:
        :param number_records:
        :param number_files:
        :param ingress:
        :param name_file:
        :return:

    """
    for i in range(0, number_files):
        data = data_itemhierarchylevelmembers(number_records)
        join_location_file_path = os.path.join(ingress, 'itemhierarchylevelmembers', name_file)
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        for j in range(0, len(file_header)):
            item_hierarchy_level_members_df[file_header[j]] = data[j]
        logger.info(f'File no: {i + 1} of {number_files}')
        item_hierarchy_level_members_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


def data_generation_create_file_items(items_df, number_records, file_header,
                                      number_files, ingress, name_file, columns_position):
    """

        This function create csv file with data provided by data_item(number_records)
        :param items_df:
        :param number_records:
        :param file_header:
        :param number_files:
        :param ingress:
        :param name_file:
        :param columns_position:
        :return:

    """
    product_group_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                                      entity='itemhierarchylevelmember',
                                                      number_of_records=str(number_records))
    for i in range(0, number_files):
        data = data_item(number_records, product_group_query)
        join_location_file_path = os.path.join(ingress, 'items', name_file)
        for j in range(0, len(columns_position)):
            items_df[file_header[columns_position[j]]] = data[j]
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        items_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


def data_generation_create_file_itemlocations(itemlocations_df, number_records, file_header,
                                              number_files, ingress, name_file, columns_position):
    """


    """
    total_records_files = number_records * number_files
    data = data_item_locations(total_records_files)
    for j in range(0, len(columns_position)):
        itemlocations_df[file_header[columns_position[j]]] = data[j]
    for i in range(0, number_files):
        itemlocations_df_temp = itemlocations_df[i * number_records:(i + 1) * number_records]
        join_location_file_path = os.path.join(ingress, 'itemlocations', name_file)
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        itemlocations_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        del itemlocations_df_temp


def data_generation_create_file_inventory_on_hand(inventoryonhand_df, number_records, file_header,
                                                  number_files, ingress, name_file, columns_position):
    """


    """
    total_records_files = number_records * number_files
    data = data_inventory_on_hand(total_records_files)
    for j in range(0, len(columns_position)):
        inventoryonhand_df[file_header[columns_position[j]]] = data[j]
    for i in range(0, number_files):
        inventoryonhand_df_temp = inventoryonhand_df[i * number_records:(i + 1) * number_records]
        join_location_file_path = os.path.join(ingress, 'inventoryonhand', name_file)
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        inventoryonhand_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        del inventoryonhand_df_temp


def data_generation_create_file_inventory_transactions(inventorytransactions_df, number_records, file_header,
                                                       number_files, ingress, name_file, columns_name):
    """


    """
    total_records_files = number_records * number_files
    data = data_inventory_transactions(total_records_files)

    for j in range(0, len(columns_name)):
        inventorytransactions_df[file_header[j]] = data[j]

    for i in range(0, number_files):
        inventorytransactions_df_temp = inventorytransactions_df[i * number_records:(i + 1) * number_records]
        join_location_file_path = os.path.join(ingress, 'inventorytransactions', name_file)
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        inventorytransactions_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        del inventorytransactions_df_temp


def data_generation_create_data_main(entity_name: str, number_records: int, number_files):
    """
            This function create csv files and save in an specific folder. We can loop depending of how many files and
            records do you need.

            :param entity_name:
            :param number_records:
            :param number_files:
            :return:
    """
    time = datetime.now()
    date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
    entity_name_ = data_generation_load_header_columns(entity_name)
    folder_paths = data_folder_ingress_processing(entity_name)
    ingress = folder_paths[0]
    file_header = entity_name_[0]
    columns_position = entity_name_[1]
    columns_name = entity_name_[2]

    if entity_name_[3] == 'locations':
        logger.info("Start locations entity file creation")
        name_file = f'locations_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_locations(df, number_files, number_records, ingress,
                                              name_file, columns_position, columns_name, file_header)
    if entity_name_[3] == 'itemhierarchylevelmembers':
        logger.info("Start itemhierarchylevelmembers entity file creation")
        name_file = f'itemhierarchylevelmembers_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_item_hierarchy_level_members(df, file_header, number_records, number_files,
                                                                 ingress, name_file)
    if entity_name_[3] == 'items':
        logger.info("Start items entity file creation")
        name_file = f'items_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_items(df, number_records, file_header,
                                          number_files, ingress, name_file, columns_position)
    if entity_name_[3] == 'itemlocations':
        logger.info("Start itemlocations entity file creation")
        name_file = f'itemlocations_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_itemlocations(df, number_records, file_header,
                                                  number_files, ingress, name_file, columns_position)
    if entity_name_[3] == 'inventoryonhand':
        logger.info("Start inventoryonhand entity file creation")
        name_file = f'inventoryonhand_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_inventory_on_hand(df, number_records, file_header,
                                                      number_files, ingress, name_file, columns_position)
    if entity_name_[3] == 'inventorytransactions':
        logger.info("Start inventorytransactions entity file creation")
        name_file = f'inventorytransactions_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_inventory_transactions(df, number_records, file_header,
                                                           number_files, ingress, name_file, columns_name)

    logger.info(f"\nEntity: {entity_name} \nNumber of records per file: {number_records} \n"
                f"Number of files: {number_files}.")
    logger.info(f"Files location: {ingress}")
