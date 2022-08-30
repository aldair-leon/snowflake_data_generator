import random
import datetime as dt
from datetime import datetime
from faker import Faker
from scripts.env_config import entity_file, data_folder
from scripts.init_logger import log
import os
from scripts.snowflake_connection import snowflake_query_ctrd_tables
from datetime import timedelta
import streamlit  as st

# Logger
logger = log('DATA GENERATION')
fake = Faker(["en_US"])


def data_folder_ingress_processing(entity: str):
    """

            This function verify and create ingress folder and processing folder, and return abspath of both folders

            :return: ingess_folder string
            :return: processing_folder string


    """
    if entity == 'items' or entity == 'locations' or entity == 'itemlocations' \
            or entity == 'inventoryonhand' or entity == 'inventorytransactions' \
            or entity == 'itemhierarchylevelmembers':
        data_folder_entity = data_folder()
        ingress_folder = os.path.join(data_folder_entity, "ingress")
        processing_folder = os.path.join(data_folder_entity, "processing")
        delta_folder = os.path.join(data_folder_entity, "delta")

        entity_folder_ingress = os.path.join(ingress_folder, entity)
        entity_folder_processing = os.path.join(processing_folder, entity)
        entity_folder_delta = os.path.join(delta_folder, entity)

        exist_ingress_folder = os.path.exists(ingress_folder)
        exist_processing_folder = os.path.exists(processing_folder)
        exist_delta_folder = os.path.exists(delta_folder)

        exist_entity_folder_ingress = os.path.exists(entity_folder_ingress)
        exist_entity_folder_processing = os.path.exists(entity_folder_processing)
        exist_processing_delta = os.path.exists(entity_folder_delta)

        if not exist_ingress_folder:
            os.makedirs(ingress_folder, exist_ok=True)
            logger.info('Creating ingress folder!')
        if not exist_processing_folder:
            os.makedirs(processing_folder, exist_ok=True)
            logger.info('Creating processing folder!')
        if not exist_delta_folder:
            os.makedirs(delta_folder, exist_ok=True)
            logger.info('Creating delta folder!')
        if not exist_entity_folder_ingress:
            os.makedirs(entity_folder_ingress, exist_ok=True)
            logger.info(f'Creating ingress/{entity} folder!')
        if not exist_entity_folder_processing:
            os.makedirs(entity_folder_processing, exist_ok=True)
            logger.info(f'Creating processing/{entity} folder!')
        if not exist_processing_delta:
            os.makedirs(entity_folder_delta, exist_ok=True)
            logger.info(f'Creating processing/{entity} folder!')
        else:
            logger.info("Folder paths are correct..")
        return ingress_folder, processing_folder, delta_folder
    else:
        logger.info('Entity name doesnt found')


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


def data_item_locations(number_records, env):
    type_list = ['AVAILABLE_FOR_SALE', 'SALE_IGNORE', 'AVAILABLE_FOR_ORDER', 'CONSTRAIN_ORDERS_DC']
    start = datetime(1999, 1, 1)
    finish = datetime(9999, 1, 1)

    item_location_query = snowflake_query_ctrd_tables(query_name='query_items_locations_join',
                                                      number_of_records=str(int(number_records)), env=env)
    if item_location_query is not None:
        item_list = item_location_query["ITEM"].tolist()
        loc_list = item_location_query["LOCATION"].tolist()
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
    else:
        return 'Not itemlocation combinations'


def data_inventory_on_hand(number_records, tran_date: datetime = dt.datetime.now(), env=''):
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(number_records),
                                           env=env)
    if len(item_loc) > 0:
        product = []
        location = []
        available = []
        unit_of_measure = []
        quantity = []
        time = []
        project = []
        store = []
        dupwarning = False
        for i in range(number_records):
            item_loc_row = i
            if i >= len(item_loc.index):
                item_loc_row = fake.random_int(min=0, max=len(item_loc.index) - 1)
                dupwarning = True
            product.append(item_loc['ITEM'][item_loc_row])
            location.append(item_loc['LOCATION'][item_loc_row])
            project.append(item_loc['LOCATION'][item_loc_row])
            store.append(item_loc['LOCATIONTYPECODE'][item_loc_row])
            quantity.append(fake.bothify(text='##'))
            time.append((tran_date + timedelta(days=i / number_records)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            available.append((tran_date + timedelta(days=i / number_records)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            unit_of_measure.append('EA')
        logger.info('finished generating inventory transaction dataset')
        if dupwarning:
            logger.warn(
                'NOTE: There were more item_on_hand records requested than there were curated item_locations in this '
                'customer realm.')
            logger.warn(
                'It is likely that you will see curation records rejected due to primary key uniqueness constraints.')
        return product, location, available, unit_of_measure, quantity, time, project, store
    else:
        if number_records == 0:
            return 'not generate Data'
        else:
            return None


def data_inventory_transactions(number_records, tran_date: datetime = dt.datetime.now(), env=''):
    item_loc = snowflake_query_ctrd_tables(query_name='query_crtd_table_item_locations',
                                           number_of_records=str(int(number_records)),
                                           env=env)

    if len(item_loc) > 0:
        item_list = []
        loc_list = []
        start_time = []
        last_sold = []
        type = []
        quantity = []
        uom = []
        salesrevenue = []
        dupwarning = False
        logger.info('Generating {0} inventory transaction records'.format(number_records))
        for i in range(number_records):
            item_loc_row = i
            if i >= len(item_loc.index):
                item_loc_row = fake.random_int(min=0, max=len(item_loc.index) - 1)
                dupwarning = True
            item_list.append(item_loc['ITEM'][item_loc_row])
            loc_list.append(item_loc['LOCATION'][item_loc_row])
            start_time.append(tran_date.strftime('%Y-%m-%d'))
            last_sold.append((tran_date + timedelta(days=i / number_records)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            type.append(random.choice(['11', '41']))
            quantity.append(fake.random_int(min=1, max=15))
            uom.append('EA')
            salesrevenue.append(fake.bothify(text='##.##'))
        logger.info('finished generating inventory transaction dataset')
        if dupwarning:
            logger.warn(
                'NOTE: There were more inventory_transaction records requested than there were curated item_locations '
                'in '
                'this customer realm.')
            logger.warn(
                'It is likely that you will see curation records rejected due to primary key uniqueness constraints.')
        return item_list, loc_list, type, quantity, uom, start_time, last_sold, salesrevenue
    else:
        if number_records == 0:
            return 'not generate Data'
        else:
            return None
