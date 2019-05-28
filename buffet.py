from __future__ import print_function

import json

from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyhive import presto
from kafka import KafkaProducer
import os
import logging
logging.basicConfig(level=logging.DEBUG)

# default_args = {'email': ['anurag.sarkar1@oyorooms.com'],
#                 'email_on_failure': True,
#                 'email_on_retry': True}
#from airflow.models import Variable
# producer = KafkaProducer(bootstrap_servers=json.loads(Variable.get('KafkaServer')), acks='all', request_timeout_ms=10000,
#                          api_version=(0, 10, 1))
producer = KafkaProducer(bootstrap_servers=['dev-kafka1.oyorooms.ms:9092'], acks='all', request_timeout_ms=10000,
                         api_version=(0, 10, 1))
conn = presto.Connection(host="presto.oyorooms.io", port=8889)
dag = DAG('hotel_buffet_data', description='hotel_buffet_data in hive to ranking service',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

path = os.getcwd()

file = os.path.join(path, 'Airflow/dags/queries/buffetdata.hql')

################ buffet score ##############
def get_hive_data_buffet(**kwargs):


    with open(file) as f:
        query = f.read()

    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    return data


def create_json_buffet(data):
    json_schema = {
        'id': str(data[0]).replace(',',''),
        'buffetBreakfast': data[4]
    }
    return json_schema


def kafka_event_buffet(**kwargs):
    ti = kwargs['ti']
    data_buffet = ti.xcom_pull(task_ids='get_hive_data_buffet')
    for d in data_buffet:
        x = create_json_buffet(d)
        producer.send('acp_corp_airflow_hotel_data', json.dumps(x))
        print(x)


hive_task_buffet = PythonOperator(
    task_id='get_hive_data_buffet',
    python_callable=get_hive_data_buffet,
    provide_context=True,
    dag=dag
)

kafka_task_buffet = PythonOperator(
    task_id='kafka_event_buffet',
    python_callable=kafka_event_buffet,
    provide_context=True,
    dag=dag
)
###########################################

op = DummyOperator(task_id='final_operator', dag=dag)

hive_task_buffet >> kafka_task_buffet >> op
