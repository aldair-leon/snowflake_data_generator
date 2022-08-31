import datetime
import time
from scripts.snowflake_connection import snowflake_query_psr
import streamlit as st
from scripts.env_config import env_streamlit_options
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


def report_parse_data_time():
    env = env_streamlit_options()
    option = st.selectbox('Report Select ENV', env[1], index=2)
    col1, col2, col3 = st.columns(3)
    with col1:
        date_start = st.date_input(
            "Start Date",
            datetime.datetime.now())
    with col2:
        time_start = st.time_input('Report Start time', datetime.time(8, 45))

    col4, col5, col6 = st.columns(3)
    with col4:
        data_finish = st.date_input(
            "Report Finish Date",
            datetime.datetime.now())
    with col5:
        time_finish = st.time_input('Report Finish time', datetime.time(8, 45))

    datetime_start = f'{date_start.strftime("%Y-%m-%d")} {time_start.strftime("%H:%M:%S")}'
    datetime_finish = f'{data_finish.strftime("%Y-%m-%d")} {time_finish.strftime("%H:%M:%S")}'

    return option, datetime_start, datetime_finish


def report_psr_entity(env, data_start, date_finish):
    query_psr = snowflake_query_psr(query_name='query_psr', env=env, data_start=data_start,
                                    date_finish=date_finish)
    query_psr_detail = snowflake_query_psr(query_name='query_psr_detail', env=env,
                                           data_start=data_start,
                                           date_finish=date_finish)
    return query_psr, query_psr_detail


def report_psr_total(env, data_start, date_finish):
    query_psr_total = snowflake_query_psr(query_name='query_psr_total', env=env, data_start=data_start,
                                          date_finish=date_finish)
    query_psr_detail_total = snowflake_query_psr(query_name='query_psr_detail_total', env=env,
                                                 data_start=data_start,
                                                 date_finish=date_finish)
    return query_psr_total, query_psr_detail_total


def report_steps_time(query_psr_detail_total):
    file_uploaded_start = query_psr_detail_total['FILE_UPLOADED_START'].to_string(index=False)
    file_uploaded_finish = query_psr_detail_total['FILE_UPLOADED_FINISH'].to_string(index=False)
    file_uploaded_start = datetime.datetime.strptime(file_uploaded_start, "%Y-%m-%d %H:%M:%S.%f")
    file_uploaded_finish = datetime.datetime.strptime(file_uploaded_finish, "%Y-%m-%d %H:%M:%S.%f")
    file_uploaded = file_uploaded_finish - file_uploaded_start

    data_ingestion_start = query_psr_detail_total['DATA_INGESTION_START'].to_string(index=False)
    data_ingestion_finish = query_psr_detail_total['DATA_INGESTION_FINISH'].to_string(index=False)
    data_ingestion_start = datetime.datetime.strptime(data_ingestion_start, "%Y-%m-%d %H:%M:%S.%f")
    data_ingestion_finish = datetime.datetime.strptime(data_ingestion_finish, "%Y-%m-%d %H:%M:%S.%f")
    data_ingestion = data_ingestion_finish - data_ingestion_start

    staging_store_start = query_psr_detail_total['STAGING_STORE_START'].to_string(index=False)
    staging_store_finish = query_psr_detail_total['STAGING_STORE_FINISH'].to_string(index=False)
    staging_store_start = datetime.datetime.strptime(staging_store_start, "%Y-%m-%d %H:%M:%S.%f")
    staging_store_finish = datetime.datetime.strptime(staging_store_finish, "%Y-%m-%d %H:%M:%S.%f")
    staging_store = staging_store_finish - staging_store_start

    validation_service_start = query_psr_detail_total['VALIDATION_SERVICE_START'].to_string(index=False)
    validation_service_finish = query_psr_detail_total['VALIDATION_SERVICE_FINISH'].to_string(index=False)
    validation_service_start = datetime.datetime.strptime(validation_service_start, "%Y-%m-%d %H:%M:%S.%f")
    validation_service_finish = datetime.datetime.strptime(validation_service_finish, "%Y-%m-%d %H:%M:%S.%f")
    validation_service = validation_service_finish - validation_service_start

    curated_store_start = query_psr_detail_total['CURATED_STORE_START'].to_string(index=False)
    curated_store_finish = query_psr_detail_total['CURATED_STORE_FINISH'].to_string(index=False)
    curated_store_start = datetime.datetime.strptime(curated_store_start, "%Y-%m-%d %H:%M:%S.%f")
    curated_store_finish = datetime.datetime.strptime(curated_store_finish, "%Y-%m-%d %H:%M:%S.%f")
    curated_store = curated_store_finish - curated_store_start

    return file_uploaded.total_seconds(), data_ingestion.total_seconds(), staging_store.total_seconds(), \
           validation_service.total_seconds(), curated_store.total_seconds()

    # time_diff = time.strptime(file_uploaded.to_string(index=False), " %H:%M:%S")


def report_comparison_entity(data):
    labels_steps_ingestion = ['ENTITY', 'FILE UPLOADED', 'DATA INGESTION', 'STAGING STORE', 'VALIDATION',
                              'CURATED STORE']
    df = pd.DataFrame(data=data, columns=labels_steps_ingestion)
    return df


@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


class DMSReport:

    def __init__(self):
        self.query_psr = pd.DataFrame({'NO_DATA': []})
        self.query_psr_detail = pd.DataFrame({'NO_DATA': []})
        self.query_psr_total = pd.DataFrame({'NO_DATA': []})
        self.query_psr_detail_total = pd.DataFrame({'NO_DATA': []})

    def report_data(self):
        st.title('PSR REPORT')
        report = report_parse_data_time()
        if report[0] == 'SELECT ENV':
            st.info('Select your ENV')
            disable = True
        else:
            disable: False
            if report[1] >= report[2]:
                st.info(f'Verify your date time,{report[1]} >= {report[2]}')
                disable = True
            else:
                disable = False
        submitted = st.button("Run", disabled=disable)
        with st.spinner('Loading..'):
            if submitted:
                df_entity = report_psr_entity(env=report[0], data_start=report[1], date_finish=report[2])
                de_total = report_psr_total(env=report[0], data_start=report[1], date_finish=report[2])
                self.query_psr = df_entity[0]
                self.query_psr_detail = df_entity[1]
                self.query_psr_total = de_total[0]
                self.query_psr_detail_total = de_total[1]
        if self.query_psr_detail.empty and self.query_psr_total.empty:
            st.info('EMPTY')
        else:
            st.subheader('Total Ingestion time')
            st.write(self.query_psr_total)
            # st.write(self.query_psr_detail_total)
            # st.subheader('Ingestion time by entity')
            # st.write(self.query_psr)
            # st.write(self.query_psr_detail)
            csv = convert_df(self.query_psr_total)

            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name='large_df.csv',
                mime='text/csv',
            )

    def report_graphs(self):
        if self.query_psr_detail.empty and self.query_psr_total.empty:
            pass
        else:
            # with st.expander('TOTAL INGESTION TIME'):
            st.header('TOTAL INGESTION OVERVIEW')
            total_curate = (int(self.query_psr_total['CURATEDRECORDS']) * 100) / int(
                self.query_psr_total['STAGEDRECORDS'])
            total_reject = (int(self.query_psr_total['REJECTEDRECORDS']) * 100) / int(
                self.query_psr_total['STAGEDRECORDS'])
            diff_time = self.query_psr_total['DIFF_HMS'].to_string(index=False)
            time_diff = time.strptime(diff_time, " %H:%M:%S")
            diff_time = datetime.timedelta(hours=time_diff.tm_hour, minutes=time_diff.tm_min,
                                           seconds=time_diff.tm_sec).total_seconds()
            avg_ingested_file = int(diff_time) / int(self.query_psr_total['NUMBER_OF_FILES'])
            avg_ingested_record = int(diff_time) / int(self.query_psr_total['STAGEDRECORDS'])
            col5, col6, col7 = st.columns(3)
            with col5:
                st.text(
                    f"INGESTION START : {self.query_psr_total['INGESTION_START'].dt.strftime('%Y-%m-%d %H:%M:%S').to_string(index=False)}")
                st.text(
                    f"INGESTION FINISH: {self.query_psr_total['INGESTION_FINISH'].dt.strftime('%Y-%m-%d %H:%M:%S').to_string(index=False)}")
                st.text(f'TOTAL INGESTION TIME : {diff_time} sec')
            with col6:
                st.text(f'CURATED RECORDS (%) : {str(round(total_curate, 3))}%')
                st.text(f'REJECTED (%) : {str(round(total_reject, 3))}%')
            with col7:
                st.text(f"AVE TIME/FILE: {avg_ingested_file} sec/file")
                st.text(f"AVE TIME/RECORD : {avg_ingested_record} sec/record")
            col1, col2, col3, col4 = st.columns(4)
            number_files = go.Figure(go.Indicator(
                mode="number",
                value=int(self.query_psr_total['NUMBER_OF_FILES']),
                domain={'x': [0.1, 1], 'y': [0.2, 0.9]},
                title={'text': "Total of files ingested"}))
            stage_records = go.Figure(go.Indicator(
                mode="number",
                value=int(self.query_psr_total['STAGEDRECORDS']),
                domain={'x': [0.1, 1], 'y': [0.2, 0.9]},
                title={'text': "Total staged records"}))
            curate_records = go.Figure(go.Indicator(
                mode="number+gauge",
                value=int(self.query_psr_total['CURATEDRECORDS']),
                domain={'x': [0.1, 1], 'y': [0.2, 0.9]},
                title={'text': "Total curated records"}))
            rejected_records = go.Figure(go.Indicator(
                mode="number",
                value=int(self.query_psr_total['REJECTEDRECORDS']),
                domain={'x': [0.1, 1], 'y': [0.2, 0.9]},
                title={'text': "Total rejected records"}))
            with col1:
                st.plotly_chart(number_files, use_container_width=True)
            with col2:
                st.plotly_chart(stage_records, use_container_width=True)
            with col3:
                st.plotly_chart(curate_records, use_container_width=True)
            with col4:
                st.plotly_chart(rejected_records, use_container_width=True)

            steps_sec = report_steps_time(self.query_psr_detail_total)

            st.write(f''' 
                        ### TOTAL INGESTION STEPS
                        |  FILE UPLOADED	| DATA INGESTION  	|  STAGING STORE  	| VALIDATION  	|  CURATED STORE |
                        |:-:	            |:-:	            |:-:	            |:-:	        |:-:	         |
                        | {round(steps_sec[0], 3)} sec 	|   {round(steps_sec[1], 3)} sec	| {round(steps_sec[2], 3)} sec|  {round(steps_sec[3], 3)} sec 	| {round(steps_sec[4], 3)}sec|
                        |{round(int(steps_sec[0]) / int(self.query_psr_total["NUMBER_OF_FILES"]), 3)} sec/file| {round(int(steps_sec[1]) / int(self.query_psr_total["NUMBER_OF_FILES"]), 3)} sec/file| {round(int(steps_sec[2]) / int(self.query_psr_total["NUMBER_OF_FILES"]), 3)} sec/file|{round(int(steps_sec[3]) / int(self.query_psr_total["NUMBER_OF_FILES"]), 3)} sec/file | {round(int(steps_sec[4]) / int(self.query_psr_total["NUMBER_OF_FILES"]), 3)} sec/file| 
                        |{round(int(steps_sec[0]) / int(self.query_psr_total["STAGEDRECORDS"]), 4)} sec/record| {round(int(steps_sec[1]) / int(self.query_psr_total["STAGEDRECORDS"]), 4)} sec/record| {round(int(steps_sec[2]) / int(self.query_psr_total["STAGEDRECORDS"]), 4)} sec/record|{round(int(steps_sec[3]) / int(self.query_psr_total["STAGEDRECORDS"]), 4)} sec/record | {round(int(steps_sec[4]) / int(self.query_psr_total["STAGEDRECORDS"]), 4)} sec/record| 

            ''')

            labels_steps_ingestion = ['FILE UPLOADED', 'DATA INGESTION', 'STAGING STORE', 'VALIDATION', 'CURATED STORE']
            steps_ingestion = go.Figure(
                data=[go.Pie(labels=labels_steps_ingestion, values=steps_sec, textinfo='label+percent',
                             insidetextorientation='radial'
                             )])
            st.plotly_chart(steps_ingestion, use_container_width=True)
            st.header('TOTAL INGESTION BY ENTITY OVERVIEW')
            entity_type = list(self.query_psr['ENTITY_TYPE'])
            time_entity = []
            entity_type_column = st.columns(len(entity_type))
            st.header('TOTAL INGESTION BY ENTITY STEPS')
            for i in range(0, len(entity_type_column)):
                total_ingestion_by_entity = str(self.query_psr.iloc[i, 7])
                time_diff_by_entity = time.strptime(total_ingestion_by_entity, "%H:%M:%S")
                total_ingestion_by_entity = datetime.timedelta(hours=time_diff_by_entity.tm_hour,
                                                               minutes=time_diff_by_entity.tm_min,
                                                               seconds=time_diff_by_entity.tm_sec).total_seconds()
                with entity_type_column[i]:
                    st.write(f'''
                                                ##### {entity_type[i]}
                                                ''')
                    st.write(f'''
                            * Ingestion Start: {self.query_psr.iloc[i, 2].strftime('%Y-%m-%d %H:%M:%S')}
                            * Ingestion Finish: {self.query_psr.iloc[i, 3].strftime('%Y-%m-%d %H:%M:%S')}
                            * Staged Records:  {self.query_psr.iloc[i, 4]} records
                            * Curated Records:  {self.query_psr.iloc[i, 5]} records
                            * Rejected Records:  {self.query_psr.iloc[i, 6]} records
                            * Ingestion time:   {total_ingestion_by_entity} sec
                            * Number of files: {self.query_psr.iloc[i, 0]}
                        ''')
                    time_entity.append(total_ingestion_by_entity)

            steps_ingestion_entity = go.Figure(
                data=[go.Pie(labels=entity_type, values=time_entity, textinfo='label+percent',
                             insidetextorientation='radial'
                             )])
            st.plotly_chart(steps_ingestion_entity, use_container_width=True)
            entity_type_steps = list(self.query_psr_detail['ENTITY_TYPE'])
            entity_type_column_steps = st.columns(len(entity_type_steps))
            comparison_df = []
            for i in range(0, len(entity_type_column_steps)):
                comparison_df_temp = []
                x = entity_type_steps[i]
                comparison_df_temp.append(x)
                df = self.query_psr_detail.query("`ENTITY_TYPE` == @x").reset_index(drop=True)
                entity_steps = report_steps_time(df)
                comparison_df_temp += list(entity_steps)
                comparison_df.append(comparison_df_temp)
                st.write(f'''
                #### {entity_type[i]}
                |  FILE UPLOADED	| DATA INGESTION  	|  STAGING STORE  	| VALIDATION  	|  CURATED STORE |
                |:-:	            |:-:	            |:-:	            |:-:	        |:-:	         |
                | {round(entity_steps[0], 3)} sec 	|   {round(entity_steps[1], 3)} sec	| {round(entity_steps[2], 3)} sec|  {round(entity_steps[3], 3)} sec 	| {round(entity_steps[4], 3)}sec|
                |{round(int(entity_steps[0]) / int(df["NUMBER_FILES"].to_string(index=False)), 3)} sec/file| {round(int(entity_steps[1]) / int(df["NUMBER_FILES"].to_string(index=False)), 3)} sec/file| {round(int(entity_steps[2]) / int(df["NUMBER_FILES"].to_string(index=False)), 3)} sec/file|{round(int(entity_steps[3]) / int(df["NUMBER_FILES"].to_string(index=False)), 3)} sec/file | {round(int(entity_steps[4]) / int(df["NUMBER_FILES"].to_string(index=False)), 3)} sec/file|
                |{round(int(entity_steps[0]) / int(df["STAGEDRECORDS"].to_string(index=False)), 4)} sec/record| {round(int(entity_steps[1]) / int(df["STAGEDRECORDS"].to_string(index=False)), 4)} sec/record| {round(int(entity_steps[2]) / int(df["STAGEDRECORDS"].to_string(index=False)), 4)} sec/record|{round(int(entity_steps[3]) / int(df["STAGEDRECORDS"].to_string(index=False)), 4)} sec/record | {round(int(entity_steps[4]) / int(df["STAGEDRECORDS"].to_string(index=False)), 4)} sec/record|
                ''')
            steps_ingestion_entity = go.Figure(
                data=[go.Pie(labels=labels_steps_ingestion, values=entity_steps, textinfo='label+percent',
                             insidetextorientation='radial'
                             )])
            st.plotly_chart(steps_ingestion_entity, use_container_width=True)
            data_comparison = report_comparison_entity(comparison_df)
            data = []
            for i in range(0, len(entity_type)):
                x = go.Bar(name=entity_type[i], x=labels_steps_ingestion, y=list(data_comparison.iloc[i, 1:]))
                data.append(x)
            comparison_graph = go.Figure(data=data)
            st.plotly_chart(comparison_graph, use_container_width=True)


start = DMSReport()
start.report_data()
start.report_graphs()
