import csv

import requests
import json
from airflow import DAG

from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

LOCAL_FOLDER = "/opt/airflow/dags/files"
HDFS_FOLDER = "currency-pairs"

BASE_CURRENCIES_FILE = "base_currencies.csv"
CURRENCY_RATES_FILE = "currency_rates.json"

DAG_SCHEDULE_INTERVAL = "*/5 * * * *"
DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "kanutan93@gmail.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

CURRENCY_RATES_TABLE = "currency_rates"

def download_currency_rates():
    fast_forex_api_conn = BaseHook.get_connection("fast_forex_api")
    fast_forex_api_key = Variable.get("fast_forex_api_key")

    with open(f"{LOCAL_FOLDER}/{BASE_CURRENCIES_FILE}") as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')

        with open(f"{LOCAL_FOLDER}/{CURRENCY_RATES_FILE}", 'w') as outfile:
            for idx, row in enumerate(reader):
                request_params = {
                    "from": row['base'],
                    "to": row['rates'],
                    "api_key": fast_forex_api_key
                }
                response_json = requests.get(f"{fast_forex_api_conn.host}/fetch-multi", params=request_params).json()
                outdata = {'base': response_json['base'], 'rates': response_json['results'],
                           'last_update': response_json['updated']}
                json.dump(outdata, outfile)
                outfile.write('\n')


with DAG("currency_pairs_data_pipeline", start_date=datetime(2024, 1, 1), schedule_interval=DAG_SCHEDULE_INTERVAL,
         default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
    is_fast_forex_api_available = HttpSensor(
        task_id="is_fast_forex_api_available",
        http_conn_id="fast_forex_api",
        endpoint="/fetch-multi",
        response_check=lambda resp: "results" in resp.text,
        poke_interval=5,
        timeout=20,
        request_params={
            "from": "USD",
            "to": "EUR",
            "api_key": Variable.get("fast_forex_api_key")
        }

    )

    is_base_currencies_file_available = FileSensor(
        task_id="is_base_currencies_file_available",
        fs_conn_id="base_currencies_path",
        filepath=BASE_CURRENCIES_FILE,
        poke_interval=5,
        timeout=20
    )

    downloading_currency_rates = PythonOperator(
        task_id="downloading_currency_rates",
        python_callable=download_currency_rates
    )

    saving_rates_to_hdfs = BashOperator(
        task_id="saving_rates_to_hdfs",
        bash_command=f"""
            hdfs dfs -mkdir -p /{HDFS_FOLDER} && \
            hdfs dfs -put -f {LOCAL_FOLDER}/{CURRENCY_RATES_FILE} /{HDFS_FOLDER}
        """
    )

    creating_currency_rates_table_in_hive = HiveOperator(
        task_id="creating_currency_rates_table_in_hive",
        hive_cli_conn_id="hive_conn",
        hql=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {CURRENCY_RATES_TABLE}(
                base STRING,
                last_update TIMESTAMP,
                eur DOUBLE,
                usd DOUBLE,
                rub DOUBLE,
                gel DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    inserting_currency_rates_to_hive = SparkSubmitOperator(
        task_id="inserting_currency_rates_to_hive",
        application=f"{LOCAL_FOLDER}/inserting_currency_rates_to_hive.py",
        conn_id="spark_conn",
        verbose=False
    )

    is_fast_forex_api_available >> is_base_currencies_file_available >> downloading_currency_rates >> saving_rates_to_hdfs
    saving_rates_to_hdfs >> creating_currency_rates_table_in_hive >> inserting_currency_rates_to_hive
