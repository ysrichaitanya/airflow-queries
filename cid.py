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
dag = DAG('hotel_cid_data', description='hotel_cid_data in hive to ranking service',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

path = os.getcwd()

file = os.path.join(path, 'Airflow/dags/queries/ciddata.hql')

################ cid score ##############
def get_hive_data_cid(**kwargs):

    with open(file) as f:
        query = f.read()

    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    return data


def create_json_cid(data):
    json_schema = {
        'id': str(data[0]).replace(',',''),
        'cid': data[2]
    }
    return json_schema


def kafka_event_cid(**kwargs):
    ti = kwargs['ti']
    data_cid = ti.xcom_pull(task_ids='get_hive_data_cid')
    for d in data_cid:
        x = create_json_cid(d)
        producer.send('acp_corp_airflow_hotel_data', json.dumps(x))
        print(x)


hive_task_cid = PythonOperator(
    task_id='get_hive_data_cid',
    python_callable=get_hive_data_cid,
    provide_context=True,
    dag=dag
)

kafka_task_cid = PythonOperator(
    task_id='kafka_event_cid',
    python_callable=kafka_event_cid,
    provide_context=True,
    dag=dag
)
###########################################

op = DummyOperator(task_id='final_operator', dag=dag)

hive_task_cid >> kafka_task_cid >> op
