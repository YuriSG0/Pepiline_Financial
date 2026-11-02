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
            url_connection = BaseHook.get_connection('exchange-rate')
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

            url_connection = BaseHook.get_connection("financial-ratio-index")
            url_requests = f'{url_connection}{financial_indicator}/dados/ultimos/{quantity_data}'
            response = requests.get(url_requests)

            return response.json()          

        ipca = requests_index_inflation.override(task_id = 'get_ipca_data')(433)
        selic = requests_index_inflation.override(task_id = 'get_selic_data')(11)
        selic_goal = requests_index_inflation.override(task_id = 'get_selic_goal_data')(432)

    with TaskGroup('Load-Exchange-Rate-Data') as load_exchange:    
        
        @task
        def loading_exchange_rate(data: Dict[str,Any]):

            df = pd.DataFrame.from_dict(data,orient='index')
            df = df.reset_index().rename(columns={'index':'par_moeda'})
            sorted_columns = [
                'par_moeda',
                'code',       
                'high',
                'low',
                'varBid',
                'pctChange',
                'bid',
                'ask',
                'create_date'
            ]
            df_sorted = df[sorted_columns]
            df_values = df_sorted.values.tolist()

            hook = SnowflakeHook(snowflake_conn_id = 'snowflake-financial')

            query = """
                INSERT INTO PUBLIC.Exange_Rate_Temp (
                par_moeda, codigo_origem, valor_maximo, valor_minimo, 
                variacao, porcentagem_mudanca, preco_compra, preco_venda, 
                data_consulta_transacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

            hook.run(query,parameters=df_values)

        loading_exchange_rate.override(task_id='loading_exchange_rate_usd_brl')(usd_brl)
        loading_exchange_rate.override(task_id='loading_exchange_rate_eur_brl')(eur_brl)
        

dag_financial()








        
        

        