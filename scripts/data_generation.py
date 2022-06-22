import random
from datetime import datetime
from faker import Faker
import pandas as pd
from scripts.env_config import entity_file, data_folder
from scripts.init_logger import log
import os
from scripts.snowflake_connection import snowflake_query_ctrd_tables

# Logger
logger = log('DATA GENERATION')
fake = Faker(["en_US"])


def data_folder_ingress_processing(entity: str):
    """

            This function verify and create ingress folder and processing folder, and return abspath of both folders

            :return: ingess_folder string
            :return: processing_folder string


    """
    data_folder_entity = data_folder()
    ingress_folder = os.path.join(data_folder_entity, "ingress")
    processing_folder = os.path.join(data_folder_entity, "processing")

    entity_folder_ingress = os.path.join(ingress_folder, entity)
    entity_folder_processing = os.path.join(processing_folder, entity)

    exist_ingress_folder = os.path.exists(ingress_folder)
    exist_processing_folder = os.path.exists(processing_folder)
    exist_entity_folder_ingress = os.path.exists(entity_folder_ingress)
    exist_entity_folder_processing = os.path.exists(entity_folder_processing)

    if not exist_ingress_folder:
        os.makedirs(ingress_folder, exist_ok=True)
        logger.info('Creating ingress folder!')
    if not exist_processing_folder:
        os.makedirs(processing_folder, exist_ok=True)
        logger.info('Creating processing folder!')
    if not exist_entity_folder_ingress:
        os.makedirs(entity_folder_ingress, exist_ok=True)
        logger.info(f'Creating ingress/{entity} folder!')
    if not exist_entity_folder_processing:
        os.makedirs(entity_folder_processing, exist_ok=True)
        logger.info(f'Creating processing/{entity} folder!')
    else:
        logger.info("Folder paths are correct..")
    return ingress_folder, processing_folder


def data_generation_load_header_columns(entity_name: str):
    """

            This function read entities.json and load columns_name, columns_position and columns_name_data depending on the
            entity that you pass.

            :param entity_name:
            :return: file_header, columns_position, columns_name_data, entity_name

    """
    entity = entity_file()
    try:
        file_header = entity[entity_name][0]['columns_name']
        columns_position = entity[entity_name][0]['columns_position']
        columns_name_data = entity[entity_name][0]['columns_name_data']
        logger.info(f'Entity name: {entity_name} --> Loading columns name')
        return file_header, columns_position, columns_name_data, entity_name
    except KeyError as e:
        logger.error(f'Entity name: {e} Doesnt exist !!')


def data_locations(number_records: int):
    """

        This function generate data for location entity. And return list of data.

        :param number_records:
        :return: locations
        :return: locations_name
        :return: location_type
        :return: country
        :return: postal_code
        :return: city
        :return: street_address
        :return: latitud_
        :return: longitud_
        :return: active_from
        :return: active_up
        :return: state

    """
    location_type_options = ['SUPPLIER', 'DISTRIBUTION_CENTER', 'STORE']
    start = datetime(1999, 1, 1)
    finish = datetime(9999, 1, 1)
    locations = [fake.bothify(text='????-#######', letters='ABCDEFGHIJK') for i in range(number_records)]
    locations_name = [fake.company() for i in range(number_records)]
    location_type = [random.choice(location_type_options) for i in range(number_records)]
    country = ['US' for i in range(number_records)]
    postal_code = [fake.building_number() for i in range(number_records)]
    city = [fake.city() for i in range(number_records)]
    street_address = [fake.street_address() for i in range(number_records)]
    latitud_ = [float(fake.latitude()) for i in range(number_records)]
    longitud_ = [float(fake.latitude()) for i in range(number_records)]
    active_from = [start for i in range(number_records)]
    active_up = [finish for i in range(number_records)]
    state = [fake.country_code() for i in range(number_records)]
    logger.info(f"{number_records} records are created for Location entity")
    return locations, locations_name, location_type, country, postal_code, city, street_address, \
           latitud_, longitud_, active_from, active_up, state


def data_itemhierarchylevelmembers(number_records):
    """

        This function generate data for itemhierarchylevelmembers entity. And return list of data.

        :param number_records:
        :return: product_group_id
        :return: parent_group_id_
        :return: description_


    """

    parent_group_id = [fake.bothify(text='####-#####-######') for i in range(int(number_records))]

    product_group_id = [fake.bothify(text='####-#####-######') for i in range(number_records)]
    description = [fake.company() for i in range(number_records)]
    parent_group_id_ = [random.choice(parent_group_id) for i in range(number_records)]
    return product_group_id, parent_group_id_, description


def data_item(number_records, product_group_query):
    """

        This function generate data for items entity. And return list of data.

        :param product_group_query:
        :param number_records:
        :return: product_group_id
        :return: product_name
        :return: product_desc
        :return: product_uom
        :return: parent_group_id_
        :return: active_from
        :return: active_up


    """
    parent_group_id = product_group_query['HIERARCHYLEVELIDENTIFIER'].tolist()
    parent_group_id_ = [random.choice(parent_group_id) for i in range(number_records)]
    start = datetime(1999, 1, 1)
    finish = datetime(9999, 1, 1)
    product_group_id = [fake.bothify(text='#############') for i in range(number_records)]
    product_name = [fake.bothify(text='#############-PRODUCT_TEST-?????-###', letters='ABCDE') for i in
                    range(number_records)]
    product_desc = product_name
    product_uom = ['EA' for i in range(number_records)]
    active_from = [start for i in range(number_records)]
    active_up = [finish for i in range(number_records)]
    return product_group_id, product_name, product_desc, product_uom, parent_group_id_, active_from, active_up


def data_item_locations(number_records, item_query, location_query):
    type_list = ['AVAILABLE_FOR_SALE', 'REPLENISHMENT']
    start = datetime(1999, 1, 1)
    finish = datetime(9999, 1, 1)
    item_list = item_query['ITEM'].tolist()
    loc_list = location_query['LOCATION'].tolist()
    item = [random.choice(item_list) for i in range(number_records)]
    location = [random.choice(loc_list) for i in range(number_records)]
    type = [random.choice(type_list) for i in range(number_records)]
    active_from = [start for i in range(number_records)]
    active_up = [finish for i in range(number_records)]
    minimumdrpqty = [1 for i in range(number_records)]
    incrementaldrpquantity = [1 for i in range(number_records)]
    minimummpsquantity = [1 for i in range(number_records)]
    incrementalmpsquantity = [1 for i in range(number_records)]
    holdingcost = [1 for i in range(number_records)]
    orderingoing = [1 for i in range(number_records)]
    costuom = [15 for i in range(number_records)]
    unitcost = ['1.99' for i in range(number_records)]
    unitmargin = ['10.55' for i in range(number_records)]
    unitprice = [25 for i in range(number_records)]
    return (item, location, type, active_from, active_up,
            minimumdrpqty, incrementaldrpquantity, minimummpsquantity,
            incrementalmpsquantity, holdingcost, orderingoing, costuom, unitcost, unitmargin, unitprice)


def data_inventory_on_hand(item_loc):
    time = datetime(2020, 1, 1)
    product = (item_loc['ITEM'].tolist())
    location = (item_loc['LOCATION'].tolist())
    available = [time for i in range(len(product))]
    unit_of_measure = ['EA' for i in range(len(product))]
    quantity = [fake.bothify(text='##') for i in range(len(product))]
    time = available
    project = location
    store = item_loc['LOCATIONTYPECODE'].tolist()
    return product, location, available, unit_of_measure, quantity, time, project, store


def data_inventory_transactions(number_records, item_loc):
    item_list = item_loc['ITEM'].tolist()
    loc_list = item_loc['LOCATION'].tolist()
    time = datetime(2020, 1, 1)

    item = [random.choice(item_list) for i in range(number_records)]
    location = [random.choice(loc_list) for i in range(number_records)]
    type = [random.choice(['11', '41']) for i in range(number_records)]
    quantity = [fake.random_int(min=1, max=15) for i in range(number_records)]
    uom = ['EA' for i in range(number_records)]
    start_time = [time for i in range(number_records)]
    last_sold = [time for i in range(number_records)]
    salesrevenue = [fake.bothify(text='##.#') for i in range(number_records)]

    return item, location, type, quantity, uom, start_time, last_sold, salesrevenue


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
    item_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                             entity='item',
                                             number_of_records=str(number_records))
    location_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                                 entity='location',
                                                 number_of_records=str(number_records))
    for i in range(0, number_files):
        data = data_item_locations(number_records, item_query, location_query)
        join_location_file_path = os.path.join(ingress, 'itemlocations', name_file)
        for j in range(0, len(columns_position)):
            itemlocations_df[file_header[columns_position[j]]] = data[j]
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        itemlocations_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


def data_generation_create_file_inventory_on_hand(inventoryonhand_df, number_records, file_header,
                                                  number_files, ingress, name_file, columns_position):
    """


    """
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(number_records))
    for i in range(0, number_files):
        data = data_inventory_on_hand(item_loc)
        join_location_file_path = os.path.join(ingress, 'inventoryonhand', name_file)
        for j in range(0, len(columns_position)):
            inventoryonhand_df[file_header[columns_position[j]]] = data[j]
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        inventoryonhand_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


def data_generation_create_file_inventory_transactions(inventorytransactions_df, number_records, file_header,
                                                       number_files, ingress, name_file, columns_name):
    """


    """
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(int(number_records)))
    for i in range(0, number_files):
        data = data_inventory_transactions(number_records, item_loc)
        join_location_file_path = os.path.join(ingress, 'inventorytransactions', name_file)
        for j in range(0, len(columns_name)):
            inventorytransactions_df[file_header[j]] = data[j]
        logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f'File no: {i + 1} of {number_files}')
        inventorytransactions_df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)


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
