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
        logger.info(f"Start {self.entity_name} entity file creation")

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

    def __init__(self, date_start, date_finish):
        self.data_start = date_start
        self.date_finish = date_finish

    def historical_data_inventoryOnhand(self, number_files_Onhand, total_records_Onhand, total_errors_Onhand):

        entity_name = 'inventoryonhand'
        time = datetime.now()
        date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
        entity_name_ = data_generation_load_header_columns(entity_name)
        folder_paths = data_folder_ingress_processing(entity_name)
        ingress = folder_paths[0]
        file_header = entity_name_[0]
        columns_position = entity_name_[1]
        total_records_files = total_records_Onhand * number_files_Onhand
        total_error_data = total_errors_Onhand * number_files_Onhand
        df = pd.DataFrame(columns=file_header)

        for date in [self.data_start + timedelta(days=x) for x in range(0, (self.date_finish - self.data_start).days)]:
            name_file = f'inventoryonhand_ISDM-2021.1.0_{date_time_str}_PSR{date.strftime("%Y%m%d")}'
            join_location_file_path = os.path.join(ingress, entity_name, name_file)
            data = data_inventory_on_hand(total_records_files)
            for j in range(0, len(columns_position)):
                df[file_header[columns_position[j]]] = data[j]
            if total_error_data > 0:
                df_error = data_inventoryonhand_error(total_error_data, file_header, df)
                for i in range(0, number_files_Onhand):
                    df_temp = df_error[
                              i * total_records_Onhand:(i + 1) * total_records_Onhand]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
                    logger.info(f'File no: {i + 1} of {number_files_Onhand}')
            else:
                for i in range(0, number_files_Onhand):
                    df_temp = df[i * total_records_Onhand:(i + 1) * total_records_Onhand]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f'File no: {i + 1} of {number_files_Onhand}')
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f"\n \t\t<---PROCESS COMPLETE--->"
                    f"\nEntity: {entity_name} \nNumber of records per file: {total_records_Onhand} \n"
                    f"Number of files per day: {number_files_Onhand} \n"
                    f"Range of date: From {self.data_start.strftime('%Y-%m-%d')} To {self.date_finish.strftime('%Y-%m-%d')}\n"
                    f"Number of error per day: {total_error_data}")

    def historical_data_inventoryTransaction(self, number_files_Transac, total_records_Transac, total_errors_Transac):
        entity_name = 'inventorytransactions'
        time = datetime.now()
        date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
        entity_name_ = data_generation_load_header_columns(entity_name)
        folder_paths = data_folder_ingress_processing(entity_name)
        ingress = folder_paths[0]
        file_header = entity_name_[0]
        columns_position = entity_name_[1]
        total_records_files = total_records_Transac * number_files_Transac
        total_error_data = total_errors_Transac * number_files_Transac
        df = pd.DataFrame(columns=file_header)
        for date in [self.data_start + timedelta(days=x) for x in range(0, (self.date_finish - self.data_start).days)]:
            name_file = f'inventorytransactions_ISDM-2021.1.0_{date_time_str}_PSR{date.strftime("%Y%m%d")}'
            join_location_file_path = os.path.join(ingress, entity_name, name_file)
            data = data_inventory_transactions(total_records_files)
            for j in range(0, len(columns_position)):
                df[file_header[columns_position[j]]] = data[j]
            if total_error_data > 0:
                df_error = data_inventoryonhand_error(total_error_data, file_header, df)
                for i in range(0, number_files_Transac):
                    df_temp = df_error[
                              i * total_records_Transac:(i + 1) * total_records_Transac]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
                    logger.info(f'File no: {i + 1} of {number_files_Transac}')
            else:
                for i in range(0, number_files_Transac):
                    df_temp = df[i * total_records_Transac:(i + 1) * total_records_Transac]
                    df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8',
                                   index=False)
                    logger.info(f'File no: {i + 1} of {number_files_Transac}')
                    logger.info(f"{join_location_file_path}{i} file created successfully ")
        logger.info(f"\n \t\t<---PROCESS COMPLETE--->"
                    f"\nEntity: {entity_name} \nNumber of records per file: {total_records_Transac} \n"
                    f"Number of files per day: {number_files_Transac} \n"
                    f"Range of date: From {self.data_start.strftime('%Y-%m-%d')} To {self.date_finish.strftime('%Y-%m-%d')}\n"
                    f"Number of error per day: {total_error_data}")

    def historical_data(self, number_files_Onhand, total_records_Onhand, total_errors_Onhand,number_files_Transac,
                        total_records_Transac, total_errors_Transac):
        self.historical_data_inventoryOnhand(number_files_Onhand, total_records_Onhand, total_errors_Onhand)
        self.historical_data_inventoryTransaction(number_files_Transac, total_records_Transac, total_errors_Transac)
