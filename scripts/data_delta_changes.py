import os
import random
import datetime as dt
from datetime import datetime
from faker import Faker
import pandas as pd
from scripts.init_logger import log
from scripts.snowflake_connection import snowflake_query_update_records
from scripts.data_generation import data_generation_load_header_columns, data_folder_ingress_processing

# Logger
logger = log('DELTA CHANGES')
fake = Faker(["en_US"])


class DeltaChanges:

    def __init__(self, entity_name, total_records, total_files):
        self.entity_name = entity_name
        self.total_records = total_records
        self.total_files = total_files

    def delta_changes(self):
        time = datetime.now()
        date_time_str = time.strftime("%Y%m%dT%H%M%SZ")
        folder_paths = data_folder_ingress_processing(self.entity_name)
        self.delta = folder_paths[2]
        self.name_file = f'{self.entity_name}_ISDM-2021.1.0_{date_time_str}_PSRTestingDelta'
        logger.info(f"Start {self.entity_name} entity file creation")
        if self.entity_name == 'inventorytransactions':
            self.delta_changes_InventoryTransaction()
        if self.entity_name == 'inventoryonhand':
            self.delta_changes_InventoryOnHand()

    def delta_changes_InventoryOnHand(self):
        total_records = self.total_files * self.total_records
        data = snowflake_query_update_records(number_of_records=total_records,
                                              query_name='query_inventory_onhand_update')
        join_location_file_path = os.path.join(self.delta, self.entity_name, self.name_file)
        for i in range(0, self.total_files):
            df_temp = data[
                      i * self.total_records:(i + 1) * self.total_records]
            '''
            
                    UPDATE DATA FRAME ......
            
            '''
            df_temp.to_csv(join_location_file_path + str(i) + '.csv', encoding='utf-8', index=False)

    def delta_changes_InventoryTransaction(self):
        self.total_records = self.total_files * self.total_records
        data = snowflake_query_update_records(number_of_records=self.total_records)
        join_location_file_path = os.path.join(self.delta, self.entity_name, self.name_file)
        data.to_csv(join_location_file_path + '.csv', encoding='utf-8', index=False)
