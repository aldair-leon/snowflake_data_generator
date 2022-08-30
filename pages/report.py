import datetime
import time
from scripts.snowflake_connection import snowflake_query_psr
import streamlit as st
from scripts.env_config import env_streamlit_options
import pandas as pd
import plotly.graph_objects as go


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
            st.write(self.query_psr_detail_total)
            st.subheader('Ingestion time by entity')
            st.write(self.query_psr)
            st.write(self.query_psr_detail)

    def report_graphs(self):
        if self.query_psr_detail.empty and self.query_psr_total.empty:
            pass
        else:
            with st.expander('TOTAL INGESTION TIME'):
                st.text(
                    f"INGESTION START : {self.query_psr_total['INGESTION_START'].dt.strftime('%Y-%m-%d %H:%M:%S').to_string(index=False)}")
                st.text(
                    f"INGESTION FINISH: {self.query_psr_total['INGESTION_FINISH'].dt.strftime('%Y-%m-%d %H:%M:%S').to_string(index=False)}")
                diff_time = self.query_psr_total['DIFF_HMS'].to_string(index=False)
                time_diff = time.strptime(diff_time, " %H:%M:%S")
                diff_time = datetime.timedelta(hours=time_diff.tm_hour, minutes=time_diff.tm_min,
                                               seconds=time_diff.tm_sec).total_seconds()
                st.text(f'TOTAL INGESTION TIME : {diff_time} sec')

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
                    mode="number+delta",
                    delta={
                        'reference': int(self.query_psr_total['STAGEDRECORDS']),
                        "valueformat": ".^1"
                    },
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

            # col1.plotly_chart(number_files, use_container_width=True)
            # col2.plotly_chart(stage_records, use_container_width=True)
            # col3.plotly_chart(curate_records, use_container_width=True)
            # col4.plotly_chart(rejected_records, use_container_width=True)


start = DMSReport()
start.report_data()
start.report_graphs()
