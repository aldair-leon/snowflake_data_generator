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
    """

            This function verify and create ingress folder and processing folder, and return abspath of both folders

            :return: ingess_folder string
            :return: processing_folder string


    """
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


def data_generation_load_header_columns(entity_name: str = 'items'):
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


def data_locations(number_records):
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
    logger.info(f"{number_records} records are created for Location entity")
    return locations, locations_name, location_type, country, postal_code, city, street_address, \
           latitud_, longitud_, active_from, active_up, state


def data_generation_create_data(entity_name: str, number_records: int, number_files):
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
    folder_paths = data_folder_ingress_processing()
    ingress = folder_paths[0]
    file_header = entity_name_[0]
    columns_position = entity_name_[1]
    columns_name = entity_name_[2]
    if entity_name_[3] != 'itemlocations':
        if entity_name_[3] == 'locations':
            logger.info("Start location entity file creation")
            name_location_file = f'locations_ISDM-2021.1.0_{date_time_str}_PSRTesting'
            locations_df = pd.DataFrame(columns=file_header)
            for i in range(0, number_files):
                data = data_locations(number_records)
                join_location_file_path = os.path.join(ingress,name_location_file)
                logger.info(f"{join_location_file_path}{i} file created successfully ")
                for j in range(0, len(columns_name)):
                    locations_df[file_header[columns_position[j]]] = data[j]
                locations_df.to_csv(join_location_file_path+str(i)+'.csv', encoding='utf-8', index=False)
    logger.info(f"\nEntity: {entity_name} \nNumber of records per file: {number_records} \n"
                f"Number of files: {number_files}.")
    logger.info(f"Files location: {ingress}")
