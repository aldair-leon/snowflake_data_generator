import random
import datetime as dt
from datetime import datetime
from datetime import timedelta
from faker import Faker
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

        :param error_data_rocords:
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
    dep = [fake.bothify(text='Department-##', letters='ABCD') for i in range(number_records)]
    caregory = [fake.bothify(text='Department-##', letters='ABC') for i in range(number_records)]
    sub_category = caregory
    price = ['1.99' for i in range(number_records)]
    return product_group_id, product_name, product_desc, product_uom, parent_group_id_, active_from, active_up, dep, caregory, sub_category, price


def data_item_locations(number_records):
    type_list = ['AVAILABLE_FOR_SALE', 'SALE_IGNORE', 'AVAILABLE_FOR_ORDER', 'CONSTRAIN_ORDERS_DC']
    start = datetime(1999, 1, 1)
    finish = datetime(9999, 1, 1)
    item_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                             entity='item',
                                             number_of_records=str(number_records))
    location_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                                 entity='location',
                                                 number_of_records=str(number_records))
    item_list = item_query['ITEM'].tolist()
    loc_list = location_query['LOCATION'].tolist()
    item = []
    location = []
    for i in range(0, len(loc_list)):
        if len(item) == number_records:
            break
        for j in range(0, len(item_list)):
            item.append(item_list[j])
            location.append(loc_list[i])
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


def data_inventory_on_hand(number_records):
    time = dt.datetime.now()
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(number_records))
    product = (item_loc['ITEM'].tolist())
    location = (item_loc['LOCATION'].tolist())
    available = [time for i in range(len(product))]
    unit_of_measure = ['EA' for i in range(len(product))]
    quantity = [fake.bothify(text='##') for i in range(len(product))]
    time = available
    project = location
    store = item_loc['LOCATIONTYPECODE'].tolist()
    return product, location, available, unit_of_measure, quantity, time, project, store


def data_inventory_transactions(number_records, tran_date: datetime):
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(int(number_records)))

    time = dt.datetime.now()

    # The total number of item_locations may be less than
    # the total number of transaction records requested. Therefore, changing this to
    # one large for loop, so that all lists are the same length at the end.
    item_list = []
    loc_list = []
    start_time = []
    last_sold = []
    type = []
    quantity = []
    uom = []
    salesrevenue = []
    logger.info('Generating {0} inventory transaction records'.format(number_records))
    for i in range(number_records):
        item_loc_row = i
        # Pick a random item_loc_row if the data request row count > total item_locations row count
        if i >= len(item_loc.index):
            item_loc_row = fake.random_int(min=0, max=len(item_loc.index)-1)

        item_list.append(item_loc['ITEM'][item_loc_row])
        loc_list.append(item_loc['LOCATION'][item_loc_row])
        start_time.append(tran_date.strftime('%Y-%m-%d'))
        last_sold.append((tran_date + timedelta(days=i/number_records)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        type.append(random.choice(['11', '41']))
        quantity.append(fake.random_int(min=1, max=15))
        uom.append('EA')
        salesrevenue.append(fake.bothify(text='##.##'))

    logger.info('finished generating inventory transaction dataset')
    return item_list, loc_list, type, quantity, uom, start_time, last_sold, salesrevenue
