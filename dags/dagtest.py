from airflow.hooks.base import BaseHook
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import requests
from datetime import datetime
import logging
import pendulum

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
    with TaskGroup('Requests for Exchange Rate Data') as extract_exchange:
        @task(trigger_rule=Trigger_Rule.ALL_DONE)
        def requests_exchange_rate_USD_Real():
            url_exchange_dolar_real = BaseHook.get_connection('ExchangeRate')
            endpoint = 'USD-BRL'
            url_requests_dolar_real = f'{url_exchange_dolar_real}{endpoint}'
            response = requests.get(url_requests_dolar_real)
            data = response.json()
            return data

        @task(trigger_rule=Trigger_Rule.ALL_DONE)
        def requests_exchange_rate_EUR_Real():
            url_exchange_euro_real = BaseHook.get_connection('ExchangeRate')
            endpoint = 'EUR-BRL'
            url_requests_euro_real = f'{url_exchange_euro_real}/{endpoint}'
            response = requests.get(url_requests_euro_real)
            data = response.json()
            return data

    with TaskGroup('Requests for Financial Ratios and Index') as extract_ratios:
        headers = {
             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             "Accept": "application/json"
        }
        @task(trigger_rule=Trigger_Rule.ALL_DONE)
        def requests_index
        
        

        