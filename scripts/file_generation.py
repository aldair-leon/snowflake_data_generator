import pandas as pd
from scripts.data_generation import data_locations, data_itemhierarchylevelmembers, data_item, data_item_locations, \
    data_inventory_on_hand, data_inventory_transactions, data_generation_load_header_columns, \
    data_folder_ingress_processing
import os
from scripts.init_logger import log
from datetime import datetime
from scripts.snowflake_connection import snowflake_query_ctrd_tables
from scripts.data_error import data_location_error, data_itemhierarchylevelmembers_error, data_items_error, \
    data_itemlocation_error, data_inventorytransactions_error, data_inventoryonhand_error

# Logger
logger = log('FILE GENERATION')


def data_generation_create_file_locations(locations_df, number_files, number_records, ingress,
                                          name_file, columns_position, columns_name, file_header, error_data_rocords):
    """

        This function create csv file with data provided by data_locations(number_records)

        :param error_data_rocords:
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
        for j in range(0, len(columns_name)):
            locations_df[file_header[columns_position[j]]] = data[j]
        if error_data_rocords > 0:
            locations_df = data_location_error(error_data_rocords, file_header, locations_df)
            logger.info(f'File no: {i + 1} of {number_files}')
            locations_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        else:
            logger.info(f'File no: {i + 1} of {number_files}')
            locations_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        logger.info(f"{join_location_file_path}{i} file created successfully ")


def data_generation_create_file_item_hierarchy_level_members(item_hierarchy_level_members_df, file_header,
                                                             number_records, number_files, ingress, name_file,
                                                             error_data_rocords):
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
        for j in range(0, len(file_header)):
            item_hierarchy_level_members_df[file_header[j]] = data[j]
        if error_data_rocords > 0:
            item_hierarchy_level_members_df = data_itemhierarchylevelmembers_error(error_data_rocords, file_header,
                                                                                   item_hierarchy_level_members_df)
            logger.info(f'File no: {i + 1} of {number_files}')
            item_hierarchy_level_members_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                                   index=False)
            logger.info(f"{join_location_file_path}{i} file created successfully ")
        else:
            logger.info(f'File no: {i + 1} of {number_files}')
            item_hierarchy_level_members_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                                   index=False)
        logger.info(f"{join_location_file_path}{i} file created successfully ")


def data_generation_create_file_items(items_df, number_records, file_header,
                                      number_files, ingress, name_file, columns_position, error_data_rocords):
    """

        This function create csv file with data provided by data_item(number_records)
        :param error_data_rocords:
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
        if error_data_rocords > 0:
            items_df = data_items_error(error_data_rocords, file_header, items_df)
            logger.info(f'File no: {i + 1} of {number_files}')
            items_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        else:
            logger.info(f'File no: {i + 1} of {number_files}')
            items_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
        logger.info(f"{join_location_file_path}{i} file created successfully ")


def data_generation_create_file_itemlocations(itemlocations_df, number_records, file_header,
                                              number_files, ingress, name_file, columns_position, error_data_rocords):
    """


    """
    total_records_files = number_records * number_files
    data = data_item_locations(total_records_files)
    join_location_file_path = os.path.join(ingress, 'itemlocations', name_file)
    for j in range(0, len(columns_position)):
        itemlocations_df[file_header[columns_position[j]]] = data[j]
    for i in range(0, number_files):
        itemlocations_df[i * number_records:(i + 1) * number_records]
        if error_data_rocords > 0:
            itemlocations_df_temp = data_itemlocation_error(error_data_rocords, file_header, itemlocations_df)
            logger.info(f'File no: {i + 1} of {number_files}')
            itemlocations_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            del itemlocations_df_temp
        else:
            logger.info(f'File no: {i + 1} of {number_files}')
            itemlocations_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            del itemlocations_df
        logger.info(f"{join_location_file_path}{i} file created successfully ")


def data_generation_create_file_inventory_on_hand(inventoryonhand_df, number_records, file_header,
                                                  number_files, ingress, name_file, columns_position,
                                                  error_data_rocords):
    """


    """
    total_records_files = number_records * number_files
    data = data_inventory_on_hand(total_records_files)
    error_data_rocords = error_data_rocords * number_files
    join_location_file_path = os.path.join(ingress, 'inventoryonhand', name_file)
    for j in range(0, len(columns_position)):
        inventoryonhand_df[file_header[columns_position[j]]] = data[j]
    if error_data_rocords > 0:
        inventoryonhand_df_error = data_inventoryonhand_error(error_data_rocords, file_header, inventoryonhand_df)
        for i in range(0, number_files):
            inventoryonhand_df_temp = inventoryonhand_df_error[i * number_records:(i + 1) * number_records]
            inventoryonhand_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            logger.info(f"{join_location_file_path}{i} file created successfully ")
            logger.info(f'File no: {i + 1} of {number_files}')
    else:
        for i in range(0, number_files):
            inventoryonhand_df_temp = inventoryonhand_df[i * number_records:(i + 1) * number_records]
            inventoryonhand_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            logger.info(f'File no: {i + 1} of {number_files}')
            del inventoryonhand_df_temp
            logger.info(f"{join_location_file_path}{i} file created successfully ")


def data_generation_create_file_inventory_transactions(inventorytransactions_df, number_records, file_header,
                                                       number_files, ingress, name_file, columns_name,
                                                       error_data_rocords, transactional_record_days_back):
    """


    """
    total_records_files = number_records * number_files
    data = data_inventory_transactions(total_records_files, transactional_record_days_back)
    error_data_rocords = error_data_rocords*number_files
    join_location_file_path = os.path.join(ingress, 'inventorytransactions', name_file)
    for j in range(0, len(columns_name)):
        inventorytransactions_df[file_header[j]] = data[j]
    if error_data_rocords > 0:
        inventorytransactions_df_error = data_inventorytransactions_error(error_data_rocords, file_header,
                                                                          inventorytransactions_df)
        for i in range(0, number_files):
            inventorytransactions_df_temp = inventorytransactions_df_error[
                                            i * number_records:(i + 1) * number_records]
            inventorytransactions_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                                 index=False)
            logger.info(f'File no: {i + 1} of {number_files}')
            logger.info(f"{join_location_file_path}{i} file created successfully ")
            del inventorytransactions_df_temp
    else:
        for i in range(0, number_files):
            inventorytransactions_df_temp = inventorytransactions_df[i * number_records:(i + 1) * number_records]
            inventorytransactions_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                                 index=False)
            logger.info(f'File no: {i + 1} of {number_files}')
            logger.info(f"{join_location_file_path}{i} file created successfully ")
            del inventorytransactions_df_temp


def data_generation_create_data_main(entity_name: str, number_records: int, number_files, error_data_rocords, transactional_record_days_back):
    """
            This function create csv files and save in an specific folder. We can loop depending of how many files and
            records do you need.

            :param error_data_rocords:
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
                                              name_file, columns_position, columns_name, file_header,
                                              error_data_rocords)
    if entity_name_[3] == 'itemhierarchylevelmembers':
        logger.info("Start itemhierarchylevelmembers entity file creation")
        name_file = f'itemhierarchylevelmembers_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_item_hierarchy_level_members(df, file_header, number_records, number_files,
                                                                 ingress, name_file, error_data_rocords)
    if entity_name_[3] == 'items':
        logger.info("Start items entity file creation")
        name_file = f'items_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_items(df, number_records, file_header,
                                          number_files, ingress, name_file, columns_position, error_data_rocords)
    if entity_name_[3] == 'itemlocations':
        logger.info("Start itemlocations entity file creation")
        name_file = f'itemlocations_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_itemlocations(df, number_records, file_header,
                                                  number_files, ingress, name_file, columns_position,
                                                  error_data_rocords)
    if entity_name_[3] == 'inventoryonhand':
        logger.info("Start inventoryonhand entity file creation")
        name_file = f'inventoryonhand_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_inventory_on_hand(df, number_records, file_header,
                                                      number_files, ingress, name_file, columns_position,
                                                      error_data_rocords)
    if entity_name_[3] == 'inventorytransactions':
        logger.info("Start inventorytransactions entity file creation")
        name_file = f'inventorytransactions_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        df = pd.DataFrame(columns=file_header)
        data_generation_create_file_inventory_transactions(df, number_records, file_header,
                                                           number_files, ingress, name_file, columns_name,
                                                           error_data_rocords, transactional_record_days_back)

    logger.info(f"\nEntity: {entity_name} \nNumber of records per file: {number_records} \n"
                f"Number of files: {number_files} \n"
                f"Number of error records in all the batch: {error_data_rocords*number_files}")
    logger.info(f"Files location: {ingress}")
