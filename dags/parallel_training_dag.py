from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'martin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def pull_data():
    print("Pulling fresh air quality data from OpenAQ...")
    print("Data pull complete.")

def train_lstm():
    print("Training LSTM model on air quality data...")
    import time
    time.sleep(3)
    print("LSTM training complete. RMSE: 0.042")

def train_arima():
    print("Training ARIMA model on air quality data...")
    import time
    time.sleep(3)
    print("ARIMA training complete. RMSE: 0.051")

def evaluate_models():
    print("Comparing LSTM vs ARIMA results...")
    print("LSTM RMSE: 0.042 — winner")
    print("Deploying LSTM model to production...")
    print("Deployment complete.")

def notify():
    print("Pipeline complete. Report sent.")

with DAG(
    dag_id='parallel_model_training',
    default_args=default_args,
    description='Parallel LSTM and ARIMA training pipeline',
    schedule_interval='0 5 * * 1',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['air-quality', 'mlops', 'parallel'],
) as dag:

    ingest = PythonOperator(
        task_id='pull_data',
        python_callable=pull_data,
    )

    lstm = PythonOperator(
        task_id='train_lstm',
        python_callable=train_lstm,
    )

    arima = PythonOperator(
        task_id='train_arima',
        python_callable=train_arima,
    )

    evaluate = PythonOperator(
        task_id='evaluate_models',
        python_callable=evaluate_models,
    )

    alert = PythonOperator(
        task_id='notify',
        python_callable=notify,
    )

    ingest >> [lstm, arima] >> evaluate >> alert
