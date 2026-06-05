import os
import re
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

PROCESSED_DATA_SET = Dataset('/opt/airflow/data/processed_data.csv')
INPUT_FILE = '/opt/airflow/data/input_data.csv'
PROCESSED_FILE = '/opt/airflow/data/processed_data.csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='1_data_transformation_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['transform']
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=300
    )

    @task.branch(task_id='check_file_content')
    def check_file_content_fn():
        if os.path.exists(INPUT_FILE) and os.path.getsize(INPUT_FILE) > 0:
            return 'data_processing_group.replace_nulls'
        return 'log_empty_file'

    log_empty = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "File is empty!"',
    )

    @task_group(group_id='data_processing_group')
    def data_processing_group():

        @task(task_id='replace_nulls')
        def replace_nulls():
            df = pd.read_csv(INPUT_FILE)
            df.fillna('-', inplace=True)
            df.to_csv(PROCESSED_FILE, index=False)

        @task(task_id='sort_by_date')
        def sort_by_date():
            df = pd.read_csv(PROCESSED_FILE)
            if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'])
                df = df.sort_values(by='at')
            df.to_csv(PROCESSED_FILE, index=False)

        @task(task_id='clean_content', outlets=[PROCESSED_DATA_SET])
        def clean_content():
            df = pd.read_csv(PROCESSED_FILE)
            def clean_text(text):
                if not isinstance(text, str):
                    return text
                return re.sub(r'[^a-zA-Zа-яА-Я0-9\s.,!?;:\-]', '', text)
            if 'content' in df.columns:
                df['content'] = df['content'].apply(clean_text)
            df.to_csv(PROCESSED_FILE, index=False)

        replace_nulls() >> sort_by_date() >> clean_content()

    wait_for_file >> check_file_content_fn() >> [log_empty, data_processing_group()]