import streamlit as st
from scripts.env_config import env_options
from datetime import datetime
import datetime
from scripts.file_generation import FileGenerationData, FileGenerationHistoricalData
from scripts.azure_blob_storage import azure_blob_upload_files

#
#
#          DATA GENERATION
#

st.title('DMS 2.0')
st.header('DATA GENERATION')
st.sidebar.success("Options")
env = env_options()
env_load = list(env[1])
env = list(env[0])
env.append('SELECT ENV')
env_load.append('SELECT ENV')
option = st.selectbox('Select Snowflake ENV', env, index=2)
with st.expander("GENERATE DATA", expanded=True):
    tab1, tab2 = st.tabs(["Custom Data", "Historical Data"])


#
#
#                 FUNCTIONS
#
#


def data_generation():
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
            active = True
        else:
            active = False
        if st.button('Create Custom Data', disabled=active):
            data_batch = FileGenerationData(entity,
                                            int(records),
                                            int(files),
                                            int(error_records),
                                            option)
            data_batch.data_generation()
            if entity == 'itemlocations':
                with st.spinner():
                    msn = data_batch.data_generation_item_loc_combinations()
                st.success('Done!')
                # st.markdown(f'	:white_check_mark: File(s) local path: \t{msn}"id_file"')
            elif entity == 'items' or entity == 'locations' or entity == 'itemhierarchylevelmembers':
                with st.spinner():
                    msn = data_batch.data_generation_master_data()
                st.success('Done!')
                # st.markdown(f'	:white_check_mark: File(s) local path: \t{msn}"id_file"')
            else:
                with st.spinner():
                    msn = data_batch.data_generation_transactional()
                st.success('Done!')
                # st.markdown(f'	:white_check_mark: File(s) local path: \t{msn}"id_file"')


def data_generation_historical():
    with tab2:
        st.subheader("Historical Data")
        st.text('Inventory Transactions')
        col1, col2, col3 = st.columns(3)
        with col1:
            files_inventoryTransactions = st.text_input('Number of files inventoryTransactions', '0')
        with col2:
            records_inventoryTransactions = st.text_input('Number of records inventoryTransactions', '0')
        with col3:
            error_records_inventoryTransactions = st.text_input('Number of error records inventoryTransactions', '0')
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
            start = st.date_input(
                "Start date",
                datetime.date(2019, 7, 6))
        with col5:
            finish = st.date_input(
                "Finish date",
                datetime.date(2019, 7, 6))
        if files_inventoryTransactions == '0' and records_inventoryTransactions == '0' and files_inventoryOnhand == '0' and records_inventoryOnhand == '0':
            st.info('Specify number of files and number od records!')
            active = True
        else:
            active = False
        if st.button('Create Historical Data', disabled=active):
            historical = FileGenerationHistoricalData(date_start=start,
                                                      date_finish=finish)
            with st.spinner():
                historical.historical_data(number_files_Onhand=int(files_inventoryOnhand),
                                           total_records_Onhand=int(records_inventoryOnhand),
                                           total_errors_Onhand=int(error_records_inventoryOnhand),
                                           number_files_Transac=int(files_inventoryTransactions),
                                           total_records_Transac=int(records_inventoryTransactions),
                                           total_errors_Transac=int(error_records_inventoryTransactions))
                st.success('Done!')


def data_ingest():
    with st.expander("INGEST DATA", expanded=True):
        entity_load = st.selectbox(
            'Ingest Entity',
            ('Select entity', 'items', 'locations', 'itemlocations', 'inventoryonhand', 'inventorytransactions',
             'itemhierarchylevelmembers'))
        if entity_load == 'Select entity':
            a = True
            st.info("Please select your entity!")
        else:
            a = False
        if st.button('Load Data', disabled=a):
            with st.spinner():
                azure_blob_upload_files(blob_container=option_load, entity=entity_load)
            st.success('Done!')

            #
            #
            # INGEST DATA
            #


if option == 'SELECT ENV':
    st.info('Please Snowflake ENV')
else:
    data_generation()
    data_generation_historical()

st.header('INGEST DATA')
option_load = st.selectbox('Select Blob Storage ENV', env_load, index=2)

if option_load == 'SELECT ENV':
    st.info('Please select Blob Storage ENV')
else:
    data_ingest()
