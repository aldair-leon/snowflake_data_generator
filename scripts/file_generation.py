import pandas as pd
from scripts.data_generation import data_locations, data_itemhierarchylevelmembers, data_item, data_item_locations, \
    data_inventory_on_hand, data_inventory_transactions, data_generation_load_header_columns, \
    data_folder_ingress_processing
import os
from scripts.init_logger import log
from datetime import datetime, timedelta
from scripts.snowflake_connection import snowflake_query_ctrd_tables
from scripts.data_error import data_location_error, data_itemhierarchylevelmembers_error, data_items_error, \
    data_itemlocation_error, data_inventorytransactions_error, data_inventoryonhand_error

# Logger
logger = log('FILE GENERATION')


class FileGenerationData:

    def __init__(self, entity_name: str, number_records: int, number_files, error_data_records, env):
        self.number_records = number_records
        self.entity_name = entity_name
        self.number_files = number_files
        self.error_data_records = error_data_records
        self.env = env

    def data_generation(self):
        time = datetime.now()
        date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
        entity_name_ = data_generation_load_header_columns(self.entity_name)
        folder_paths = data_folder_ingress_processing(self.entity_name)
        self.ingress = folder_paths[0]
        self.file_header = entity_name_[0]
        self.columns_position = entity_name_[1]
        self.columns_name = entity_name_[2]
        self.name_file = f'{self.entity_name}_ISDM-2021.1.0_{date_time_str}_PSRTesting'
        self.df = pd.DataFrame(columns=self.file_header)
        logger.info("Start locations entity file creation")

    def data_generation_master_data(self):
        if self.entity_name == 'items' or self.entity_name == 'locations' or self.entity_name == 'itemhierarchylevelmembers':
            for i in range(0, self.number_files):
                if self.entity_name == 'locations':
                    data = data_locations(self.number_records)
                if self.entity_name == 'items':
                    product_group_query = snowflake_query_ctrd_tables(query_name='query_crtd_table_entity',
                                                                      entity='itemhierarchylevelmember',
                                                                      number_of_records=str(self.number_records),
                                                                      env=self.env)
                    data = data_item(self.number_records, product_group_query)
                if self.entity_name == 'itemhierarchylevelmembers':
                    data = data_itemhierarchylevelmembers(self.number_records)
                join_location_file_path = os.path.join(self.ingress, self.entity_name, self.name_file)
                for j in range(0, len(self.columns_name)):
                    self.df[self.file_header[self.columns_position[j]]] = data[j]
                if self.error_data_records > 0:
                    if self.entity_name == 'locations':
                        df_final = data_location_error(self.error_data_records, self.file_header, self.df)
                    if self.entity_name == 'items':
                        df_final = data_items_error(self.error_data_records, self.file_header, self.df)
                    if self.entity_name == 'itemhierarchylevelmembers':
                        df_final = data_itemhierarchylevelmembers_error(self.error_data_records, self.file_header,
                                                                        self.df)
                    logger.info(f'File no: {i + 1} of {self.number_files}')
                    df_final.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
                else:
                    logger.info(f'File no: {i + 1} of {self.number_files}')
                    self.df.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            logger.info(f"{join_location_file_path}{i} file created successfully ")
            logger.info(f"\n \t\t<---PROCESS COMPLETE--->"
                        f"\nEntity: {self.entity_name} \nNumber of records per file: {self.number_records} \n"
                        f"Number of files: {self.number_files} \n"
                        f"Number of error records in all the batch: {self.error_data_records * self.number_files}")
        else:
            logger.info('\n\t<-----------------------------WARNING !--------------------------------------->'
                        '\nPlease enter the correct entity name this function only accept -> items, locations or '
                        'itemhierarchylevelmembers')

    def data_generation_item_loc_combinations(self):
        if self.entity_name == 'itemlocations':
            total_records_files = self.number_records * self.number_files
            error_data_rocords = self.error_data_records * self.number_files
            data = data_item_locations(total_records_files)
            join_location_file_path = os.path.join(self.ingress, self.entity_name, self.name_file)
            for j in range(0, len(self.columns_position)):
                self.df[self.file_header[self.columns_position[j]]] = data[j]
            for i in range(0, self.number_files):
                if self.error_data_records > 0:
                    itemlocations_df_temp = data_itemlocation_error(error_data_rocords, self.file_header,
                                                                    self.df[
                                                                    i * self.number_records:(
                                                                                                    i + 1) * self.number_records].copy())
                    logger.info(f'File no: {i + 1} of {self.number_files}')
                    itemlocations_df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                                 index=False)
                else:
                    logger.info(f'File no: {i + 1} of {self.number_files}')
                    self.df[i * self.number_records:(i + 1) * self.number_records].to_csv(
                        join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)
            logger.info(f"{join_location_file_path}{i} file created successfully ")
            logger.info(f"\n \t\t<---PROCESS COMPLETE--->"
                        f"\nEntity: {self.entity_name} \nNumber of records per file: {self.number_records} \n"
                        f"Number of files: {self.number_files} \n"
                        f"Number of error records in all the batch: {self.error_data_records * self.number_files}")
        else:
            logger.info('\n\t<-----------------------------WARNING !--------------------------------------->'
                        '\nPlease enter the correct entity name, this function only accept -> itemlocations')

    def data_generation_transactional(self):
        if self.entity_name == 'inventoryonhand' or self.entity_name == 'inventorytransactions':
            total_records_files = self.number_records * self.number_files
            error_data_records = self.error_data_records * self.number_files
            join_location_file_path = os.path.join(self.ingress, self.entity_name, self.name_file)

            if self.entity_name == 'inventorytransactions':
                data = data_inventory_transactions(total_records_files)
            if self.entity_name == 'inventoryonhand':
                data = data_inventory_on_hand(total_records_files)
            for j in range(0, len(self.columns_position)):
                self.df[self.file_header[self.columns_position[j]]] = data[j]
            if error_data_records > 0:
                if self.entity_name == 'inventoryonhand':
                    df_error = data_inventoryonhand_error(error_data_records, self.file_header, self.df)
                if self.entity_name == 'inventorytransactions':
                    df_error = data_inventorytransactions_error(error_data_records, self.file_header, self.df)
                for i in range(0, self.number_files):
                    df_temp = df_error[
                              i * self.number_records:(i + 1) * self.number_records]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
                    logger.info(f'File no: {i + 1} of {self.number_files}')
            else:
                for i in range(0, self.number_files):
                    df_temp = self.df[i * self.number_records:(i + 1) * self.number_records]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f'File no: {i + 1} of {self.number_files}')
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
            logger.info(f"\n \t\t<---PROCESS COMPLETE--->"
                        f"\nEntity: {self.entity_name} \nNumber of records per file: {self.number_records} \n"
                        f"Number of files: {self.number_files} \n"
                        f"Number of error records in all the batch: {self.error_data_records * self.number_files}")
        else:
            logger.info('\n\t<-----------------------------WARNING !--------------------------------------->'
                        '\nPlease enter the correct entity name, this function only accept -> inventoryonhand  or '
                        'inventorytransactions')


class FileGenerationHistoricalData:

    def __init__(self, date_start, date_finish, number_files):
        self.data_start = date_start
        self.date_finish = date_finish
        self.number_files = number_files

    def test(self):
        time = datetime.now()
        date_time_str = time.strftime("%Y%m%dT%H%M%SZ")

        for date in [self.data_start + timedelta(days=x) for x in
                     range(0, (self.date_finish - self.data_start).days)]:
            name_file = f'inventoryonhand_ISDM-2021.1.0_{date_time_str}_PSR_{date.strftime("%Y%m%d")}'

