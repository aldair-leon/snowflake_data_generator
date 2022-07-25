"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

from scripts.file_generation import FileGeneration

entity_name = 'inventorytransactions'
number_records = 10
number_files = 2
error_data_records = 5
env = 'DEV_PSR_ACCOUNT'

data_batch = FileGeneration(entity_name, number_records, number_files, error_data_records, env)
data_batch.data_generation()
# data_batch.data_generation_master_data()  # items, locations , itemhierarchylevelmembers
# data_batch.data_generation_item_loc_combinations()  # itemlocations
data_batch.data_generation_transactional()  # inventoryonhand, inventorytransactions
