from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'martin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def pull_air_quality_data():
    print("Pulling fresh air quality data from OpenAQ...")
    print("Data pull complete.")

def retrain_model():
    print("Retraining air quality model with new data...")
    print("Model retrained successfully.")

def evaluate_model():
    print("Evaluating new model against baseline...")
    print("Evaluation complete.")

with DAG(
    dag_id='air_quality_retrain',
    default_args=default_args,
    description='Weekly air quality model retraining pipeline',
    schedule_interval='0 5 * * 1',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['air-quality', 'mlops'],
) as dag:

    pull_data = PythonOperator(
        task_id='pull_data',
        python_callable=pull_air_quality_data,
    )

    retrain = PythonOperator(
        task_id='retrain_model',
        python_callable=retrain_model,
    )

    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )

    pull_data >> retrain >> evaluate
