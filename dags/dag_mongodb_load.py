from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_DATA_SET = Dataset('/opt/airflow/data/processed_data.csv')
PROCESSED_FILE = '/opt/airflow/data/processed_data.csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='2_load_to_mongodb',
    default_args=default_args,
    schedule=[PROCESSED_DATA_SET], 
    catchup=False,
    tags=['mongodb']
) as dag:

    @task(task_id='load_data_to_mongo')
    def load_to_mongo_fn():
      
        df = pd.read_csv(PROCESSED_FILE)
        
        records = df.to_dict(orient='records')
        
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        
        db = client['airflow_db']
        collection = db['processed_comments']
        
        if records:
            collection.insert_many(records)
            print(f"Successfully loaded {len(records)} records into MongoDB.")
        else:
            print("No data to load.")

    load_to_mongo_fn()