"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.file_generation import FileGenerationData, FileGenerationHistoricalData
from datetime import datetime
from scripts.azure_blob_storage import azure_blob_upload_files

'''

                                Data creation for all the entities Master and Transactional

        1) FileGenerationData -> Principal Object

        2) data_batch.data_generation() -> Build all variables and configurations for your data. This method must
                                           run first.
        3) data_batch.data_generation_master_data() -> This method create master data from Items, Locations and
                                                        Itemhierarchylevelmembers
        4) data_batch.data_generation_item_loc_combinations() -> This method generated data only for itemlocations

        5) data_batch.data_generation_transactional() -> This method generated transactional data inventoryonhand,
                                                         inventorytransactions

'''

entity_name = 'inventorytransactions'
number_records = 100000
number_files = 1
error_data_records = 0
env = 'DEV_PSR_ACCOUNT'  # DEV_PSR_ACCOUNT_EU --> Dedicated env
env_blob_containe = 'DEV_PSR'  # DEV_PSR_EU --> Dedicated env

data_batch = FileGenerationData(entity_name,
                                number_records,
                                number_files,
                                error_data_records,
                                env)  # 1

# data_batch.data_generation()  # 2
# data_batch.data_generation_master_data()            # 3
# data_batch.data_generation_item_loc_combinations()  # 4
# data_batch.data_generation_transactional()          # 5

# azure_blob_upload_files(blob_container=env_blob_containe, entity=entity_name)

'''
                                Historical data only for transactional entities

'''

transactional_records_start = datetime.strptime('2022-07-03', '%Y-%m-%d')
transactional_records_end = datetime.strptime('2022-07-04', '%Y-%m-%d')

historical = FileGenerationHistoricalData(date_start=transactional_records_start,
                                          date_finish=transactional_records_end)
historical.historical_data(number_files_Onhand=1, total_records_Onhand=1, total_errors_Onhand=0,
                           number_files_Transac=0, total_records_Transac=0, total_errors_Transac=0)
