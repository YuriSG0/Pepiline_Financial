from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import requests
from datetime import datetime
import logging
import pendulum
from typing import Any, Dict
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

@dag(
    dag_id = 'dag_financial_data',
    description = 'dag with objective test connection using basehook the airflow',
    schedule=None,
    catchup=False,
    start_date = pendulum.datetime(2026,10,24,tz='America/Sao_Paulo')
)

def dag_financial():

    with TaskGroup('Requests-for-Exchange-Rate-Data') as extract_exchange:
        @task(trigger_rule= TriggerRule.ALL_DONE)
        def requests_exchange_rate(currency_pair:str):
            url_connection = BaseHook.get_connection('exchange-rate').host
            url_requests = f'{url_connection}/{currency_pair}'
            response = requests.get(url_requests)

            return response.json()

        usd_brl = requests_exchange_rate.override(task_id = 'get_usd_brl')('USD-BRL')
        eur_brl = requests_exchange_rate.override(task_id = 'get_eur_brl')('EUR-BRL')

    with TaskGroup('Requests-for-Financial-Ratios-and-Index') as extract_ratios_index:

        headers_Bacen = {
             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             "Accept": "application/json"
        }       

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def requests_index_inflation(financial_indicator:str,quantity_data=20):

            url_connection = BaseHook.get_connection("financial-ratio-index").host
            url_requests = f'{url_connection}{financial_indicator}/dados/ultimos/{quantity_data}'
            response = requests.get(url_requests)

            return response.json()          

        ipca = requests_index_inflation.override(task_id = 'get_ipca_data')(433)
        selic = requests_index_inflation.override(task_id = 'get_selic_data')(11)
        selic_goal = requests_index_inflation.override(task_id = 'get_selic_goal_data')(432)

    with TaskGroup('Loading-for-Tables-Snowflake') as loading_data_financial:

        @task
        def loading_data_index_inflation(data_index_inflation:dict,indicator:str):

            df = pd.DataFrame(data_index_inflation)
            df['indicador'] = indicator
            df = df[['data','valor','indicador']]

            values = df.values.tolist()

            hook = SnowflakeHook(snowflake_conn_id ='snowflake-financial')

            hook.insert_rows(
                table = 'Financial_Indicator_Temp',
                rows = values,
                target_fields=['date_visit','value_indicator','indicator']
            )

        load_ipca_data = loading_data_index_inflation.override(task_id='load_ipca_data')(ipca,'IPCA')
        
dag_financial()








        
        

        