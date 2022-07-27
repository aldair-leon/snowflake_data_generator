"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.file_generation import FileGenerationData, FileGenerationHistoricalData
from datetime import datetime

entity_name = 'inventoryonhand'
number_records = 100
number_files = 1
error_data_records = 0
env = 'DEV_PSR_ACCOUNT'

data_batch = FileGenerationData(entity_name, number_records, number_files, error_data_records, env)
# data_batch.data_generation()
# data_batch.data_generation_master_data()  # items, locations , itemhierarchylevelmembers
# data_batch.data_generation_item_loc_combinations()  # itemlocations
# data_batch.data_generation_transactional()  # inventoryonhand, inventorytransactions


# note inventory transaction & inventory_on_hand will generate the above number of records
# and files for each day in the range below
transactional_records_start = datetime.strptime('2022-07-14', '%Y-%m-%d')
transactional_records_end = datetime.strptime('2022-07-18', '%Y-%m-%d')

historical = FileGenerationHistoricalData(date_start=transactional_records_start, date_finish=transactional_records_end,
                                          number_files=2, total_errors=5, total_records=10)
historical.historical_data_inventoryOnhand()
