import random
from datetime import datetime
from faker import Faker
import pandas as pd
from scripts.env_config import entity_file, data_folder
from scripts.init_logger import log
import os

# Logger
logger = log('DATA GENERATION')
fake = Faker(["en_US"])


def data_folder_ingress_processing():
    data_folder_entity = data_folder()
    ingress_folder = os.path.join(data_folder_entity, "ingress")
    processing_folder = os.path.join(data_folder_entity, "processing")
    exist_ingress_folder = os.path.exists(ingress_folder)
    exist_processing_folder = os.path.exists(processing_folder)
    if not exist_ingress_folder:
        os.makedirs(ingress_folder, exist_ok=True)
        logger.info('Creating ingress folder!')
    if not exist_processing_folder:
        os.makedirs(processing_folder, exist_ok=True)
        logger.info('Creating processing folder!')
    else:
        logger.info("Folder paths are correct..")
    return ingress_folder, processing_folder


def data_generation_select_entity(entity_name: str = 'items'):
    entity = entity_file()
    try:
        file_header = entity[entity_name][0]['columns_name']
        columns_position = entity[entity_name][0]['columns_position']
        columns_name_data = entity[entity_name][0]['columns_name_data']
        logger.info(f'Entity name: {entity_name} --> Loading columns name')
        return file_header, columns_position, columns_name_data, entity_name
    except KeyError as e:
        logger.error(f'Entity name: {e} Doesnt exist !!')


def data_locations(number_records):
    location_type_options = ['SUPPLIER', 'DISTRIBUTION_CENTER', 'STORE']
    start = datetime(1999, 1, 1)
    finish = datetime(2100, 1, 1)
    locations = [fake.bothify(text='???-####', letters='ABCDEF') for i in range(number_records)]
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
    return locations, locations_name, location_type, country, postal_code, city, street_address, \
           latitud_, longitud_, active_from, active_up, state


def data_generation_create_data(entity_name: str, number_records: int, number_files):
    time = datetime.now()
    date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
    entity_name = data_generation_select_entity(entity_name)
    folder_paths = data_folder_ingress_processing()
    ingress = folder_paths[0]
    file_header = entity_name[0]
    columns_position = entity_name[1]
    columns_name = entity_name[2]
    if entity_name[3] != 'itemlocations':
        if entity_name[3] == 'locations':
            name_location_file = f'locations_ISDM-2021.1.0_{date_time_str}_PSRTesting'
            locations_df = pd.DataFrame(columns=file_header)
            for i in range(0, number_files):
                data = data_locations(number_records)
                join_location_file_path = os.path.join(ingress,name_location_file)
                for j in range(0, len(columns_name)):
                    locations_df[file_header[columns_position[j]]] = data[j]
                locations_df.to_csv(join_location_file_path+str(i)+'.csv', encoding='utf-8', index=False)


data_generation_create_data('locations', 100000, 1)
