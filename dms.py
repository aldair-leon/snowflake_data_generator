from scripts.snowflake_connection import snowflake_query_stats_table
from scripts.env_config import snowflake_account_blob_storage
import streamlit as st
from scripts.file_generation import *
from scripts.azure_blob_storage import azure_blob_upload_files
from scripts.env_config import data_folder, env_streamlit_options

# MAIN CONFIG
st.set_page_config(page_title="DATA GENERATION",
                   page_icon=":bar_chart:", layout="wide")
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.sidebar.success("Options")
st.markdown(hide_st_style, unsafe_allow_html=True)


class DMSDataGeneration:

    def __init__(self):
        pass

    def data_generation(self):
        with tab1:
            st.subheader("Custom Data")
            entity = st.selectbox(
                'Select entity',
                ('Select entity', 'items', 'locations', 'itemlocations', 'inventoryonhand', 'inventorytransactions',
                 'itemhierarchylevelmembers'))

            col1, col2, col3 = st.columns(3)
            with col1:
                files = st.text_input('Number of files', '0')
            with col2:
                records = st.text_input('Number of records', '0')
            with col3:
                error_records = st.text_input('Number of error Records', '0')
            if files == '0' or records == '0' or entity == 'Select entity':
                st.info('Specify number of files , number of records and entity!')
                disabled = True
            else:
                disabled = False
            if st.button('Create Custom Data', disabled=disabled):
                data_batch = FileGenerationData(entity,
                                                int(records),
                                                int(files),
                                                int(error_records),
                                                option)
                data_batch.data_generation()
                if entity == 'itemlocations':
                    with st.spinner():
                        self.msn = data_batch.data_generation_item_loc_combinations()
                        if self.msn != 'Not itemlocation combinations':
                            st.success('Done!')
                        else:
                            st.error('Not itemlocation combinations data  created please verify data in CRTD_ITEM and '
                                     'CRTD_LOCATION')
                elif entity == 'items' or entity == 'locations' or entity == 'itemhierarchylevelmembers':
                    with st.spinner():
                        self.msn = data_batch.data_generation_master_data()
                    st.success('Done!')
                else:
                    with st.spinner():
                        self.msn = data_batch.data_generation_transactional()
                        if self.msn is not None:
                            st.warning(self.msn)
                        else:
                            st.success('Done')

    def data_generation_historical(self):
        with tab2:
            st.subheader("Historical Data")
            st.text('Inventory Transactions')
            col1, col2, col3 = st.columns(3)
            with col1:
                files_inventoryTransactions = st.text_input('Number of files inventoryTransactions', '0')
            with col2:
                records_inventoryTransactions = st.text_input('Number of records inventoryTransactions', '0')
            with col3:
                error_records_inventoryTransactions = st.text_input('Number of error records inventoryTransactions',
                                                                    '0')
            st.text('Inventory Onhand')
            col1_, col2_, col3_ = st.columns(3)
            with col1_:
                files_inventoryOnhand = st.text_input('Number of files inventoryOnhand', '0')
            with col2_:
                records_inventoryOnhand = st.text_input('Number of records inventoryOnhand', '0')
            with col3_:
                error_records_inventoryOnhand = st.text_input('Number of error records inventoryOnhand', '0')

            col4, col5 = st.columns(2)
            with col4:
                start_hist = st.date_input(
                    "Start date",
                    datetime.now())
            with col5:
                finish_hist = st.date_input(
                    "Finish date",
                    datetime.now())
            if files_inventoryTransactions == '0' and records_inventoryTransactions == '0' and files_inventoryOnhand == '0' and records_inventoryOnhand == '0':
                st.info('Specify number of files and number od records!')
                disabled = True
            else:
                disabled = False
            if st.button('Create Historical Data', disabled=disabled):
                historical = FileGenerationHistoricalData(date_start=start_hist,
                                                          date_finish=finish_hist,
                                                          env=option)
                with st.spinner():
                    self.msn = historical.historical_data(number_files_Onhand=int(files_inventoryOnhand),
                                                          total_records_Onhand=int(records_inventoryOnhand),
                                                          total_errors_Onhand=int(error_records_inventoryOnhand),
                                                          number_files_Transac=int(files_inventoryTransactions),
                                                          total_records_Transac=int(records_inventoryTransactions),
                                                          total_errors_Transac=int(error_records_inventoryTransactions))

                    if self.msn[0] == 'Not data in CRTD_ITEMLOCATIONS for inventoryOnhand please verify your DB':
                        st.warning(self.msn[0])
                    if self.msn[0] == 'No data generated for InventoryOnhand':
                        st.warning(self.msn[0])
                    if self.msn[1] == 'Not data in CRTD_ITEMLOCATIONS for inventoryTransaction please verify your DB':
                        st.warning(self.msn[1])
                    if self.msn[1] == 'No data generated for InventoryTransactions':
                        st.warning(self.msn[1])
                    else:
                        st.success('Done!')

    def data_ingest(self):
        with st.expander("INGEST DATA", expanded=True):
            snowflake_account = snowflake_account_blob_storage(option_load)
            entity_load = st.selectbox(
                'Ingest Entity',
                ('Select entity', 'items', 'locations', 'itemlocations', 'inventoryonhand', 'inventorytransactions',
                 'itemhierarchylevelmembers'))
            if entity_load != 'Select entity':
                data_folder_path = data_folder()
                data_folder_path_ingress = os.path.join(data_folder_path, 'ingress', entity_load)
                files = os.listdir(data_folder_path_ingress)
                file_select = st.multiselect(label='Select your files',
                                             options=files)
            if entity_load == 'Select entity' or file_select == []:
                disabled = True
                st.info("Please select your entity and file(s)!")
            else:
                disabled = False
            if st.button('Load Data', disabled=disabled):
                with st.spinner():
                    azure_blob_upload_files(blob_container=option_load, entity=entity_load, file_list=file_select)

                st.success('Done!')

        with st.expander("TRACK INGESTION", expanded=False):
            if entity_load != 'Select entity':
                data_folder_path_proc = data_folder()
                data_folder_path_processing = os.path.join(data_folder_path_proc, 'processing', entity_load)
                files_processing = os.listdir(data_folder_path_processing)
                file_select_processing = st.multiselect(label='Select ingestion tracking file',
                                                        options=files_processing)
                if not file_select_processing:
                    st.warning('No data!')
                else:
                    with st.spinner():
                        if st.button('Update'):
                            x = snowflake_query_stats_table(query_name='query_ingestion_update',
                                                            files=file_select_processing,
                                                            env=snowflake_account)
                            status = ['FILE_UPLOADED', 'DATA_INGESTION_STARTED',
                                      'DATA_MERGED_FROM_STAGE_INTO_STAGING_STORE',
                                      'DATA_VALIDATIONS_DONE', 'DATA_MERGED_FROM_STAGING_INTO_CURATED_STORE']
                            for i in range(0, len(file_select_processing)):
                                file = file_select_processing[i]
                                for j in range(0, len(status)):
                                    x_new = x.query('@file==`FILENAME`  and STATUS in @status').reset_index(drop=True)
                                if not x_new.empty:
                                    msn = x_new.head(1)
                                    if msn['STATUS'].item() == 'DATA_MERGED_FROM_STAGING_INTO_CURATED_STORE':
                                        emoji = ':heavy_check_mark:'
                                    else:
                                        emoji = ':x:'
                                    st.write(f'''
                                        |File name|Status | |
                                        |:-:	|   :-:	| :-:  |                              
                                        |{msn['FILENAME'].to_string(index=False)}|  {msn['STATUS'].to_string(index=False)}|{emoji} |
                                         ''')
                        # if len(x) % 3 != 0:
                        #     y = len(x) % 3
                        #     x_new = x.tail(y).copy().reset_index(drop=True)
                        #     x.drop(x.tail(y).index, inplace=True)
                        #     for i in range(0, len(x)):
                        #         if (i + 1) % 3 == 0:
                        #             df = x[i - 2:i + 1].copy().reset_index(drop=True)
                        #             columns_list_filename_ = list(df['FILENAME'])
                        #             columns_list_filename = st.columns(len(columns_list_filename_))
                        #             for j in range(0, len(columns_list_filename)):
                        #                 with columns_list_filename[j]:
                        #                     st.write(f'''
                        #                     |   	|
                        #                     |:-:	|
                        #                     |File name: {df.loc[j, 'FILENAME']}|
                        #                     |Ingestion Start: {df.loc[j, 'INGESTION_START']}|
                        #                     | Ingestion Finish: {df.loc[j, 'INGESTION_FINISH']}|
                        #                     | Elapse Time: {df.loc[j, 'DIFF_HMS']}|
                        #                     |  Staged Records: {df.loc[j, 'STAGEDRECORDS']}|
                        #                     |  Curated Records: {df.loc[j, 'CURATEDRECORDS']}|
                        #                     |  Rejected Records: {df.loc[j, 'REJECTRECORDS']}|
                        #                     |  Validation Status: {df.loc[j, 'STATUS_VALIDATION']}|
                        #                     |  Error: {df.loc[j, 'ERROR_MESSAGE']}|
                        #                     ''')
                        #             st.empty()
                        #     col = st.columns(y)
                        #     for i in range(0, len(x_new)):
                        #         with col[i]:
                        #             st.write(f'''
                        #             |   	|
                        #             |:-:	|
                        #             |File name: {x_new.loc[i, 'FILENAME']}|
                        #             |Ingestion Start: {x_new.loc[i, 'INGESTION_START']}|
                        #             | Ingestion Finish: {x_new.loc[i, 'INGESTION_FINISH']}|
                        #             | Elapse Time: {x_new.loc[i, 'DIFF_HMS']}|
                        #             |  Staged Records: {x_new.loc[i, 'STAGEDRECORDS']}|
                        #             |  Curated Records: {x_new.loc[i, 'CURATEDRECORDS']}|
                        #             |  Rejected Records: {x_new.loc[i, 'REJECTRECORDS']}|
                        #             |  Validation Status: {x_new.loc[i, 'STATUS_VALIDATION']}|
                        #             |  Error: {x_new.loc[i, 'ERROR_MESSAGE']}|
                        #             ''')
                        #     st.empty()
                        #
                        # else:
                        #     for i in range(0, len(x)):
                        #         if (i + 1) % 3 == 0:
                        #             df = x[i - 2:i + 1].copy().reset_index(drop=True)
                        #             columns_list_filename_ = list(df['FILENAME'])
                        #             columns_list_filename = st.columns(len(columns_list_filename_))
                        #             for j in range(0, len(columns_list_filename)):
                        #                 with columns_list_filename[j]:
                        #                     st.write(f'''
                        #                     |   	|
                        #                     |:-:	|
                        #                     |File name: {df.loc[j, 'FILENAME']}|
                        #                     |Ingestion Start: {df.loc[j, 'INGESTION_START']}|
                        #                     | Ingestion Finish: {df.loc[j, 'INGESTION_FINISH']}|
                        #                     | Elapse Time: {df.loc[j, 'DIFF_HMS']}|
                        #                     |  Staged Records: {df.loc[j, 'STAGEDRECORDS']}|
                        #                     |  Curated Records: {df.loc[j, 'CURATEDRECORDS']}|
                        #                     |  Rejected Records: {df.loc[j, 'REJECTRECORDS']}|
                        #                     |  Validation Status: {df.loc[j, 'STATUS_VALIDATION']}|
                        #                     |  Error: {df.loc[j, 'ERROR_MESSAGE']}|
                        #                     ''')
                        #             st.empty()
                        #


st.title('DMS 2.0')
start = DMSDataGeneration()
env = env_streamlit_options()
option = st.selectbox('Select Snowflake ENV', env[1], index=2)
st.header('DATA GENERATION')
with st.expander("GENERATE DATA", expanded=True):
    tab1, tab2 = st.tabs(["Custom Data", "Historical Data"])
if option == 'SELECT ENV':
    st.info('Please Snowflake ENV')
else:
    start.data_generation()
    start.data_generation_historical()
st.header('INGEST DATA')
option_load = st.selectbox('Select Blob Storage ENV', env[0], index=2)

if option_load == 'SELECT ENV':
    st.info('Please select Blob Storage ENV')
else:
    start.data_ingest()
