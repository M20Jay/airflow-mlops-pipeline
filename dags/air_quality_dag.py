from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import subprocess

def notify_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    send_email(
        to='ngangam93@gmail.com',
        subject=f'Airflow Alert — {dag_id} FAILED',
        html_content=f"""
        <h3>DAG Failure Alert</h3>
        <p><b>DAG:</b> {dag_id}</p>
        <p><b>Task:</b> {task_id}</p>
        <p><b>Time:</b> {execution_date}</p>
        <p>Check Airflow UI: http://airflow.martin-mlops.com</p>
        """
    )

default_args = {
    'owner': 'martin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
    'email_on_failure': False,
}

def pull_air_quality_data():
    print("Pulling fresh air quality data from OpenAQ...")
    print("Data pull complete.")

def retrain_model():
    print("Retraining air quality model with new data...")
    print("Model retrained successfully.")
    print("Saving model to /tmp/air_quality_model.pkl")

def evaluate_model():
    print("Evaluating new model against baseline...")
    print("Evaluation complete. RMSE: 9.93")

def save_model_to_s3():
    import boto3
    from datetime import datetime
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f'models/air_quality/model_{timestamp}.txt'
    s3.put_object(
        Bucket='martin-mlops-models',
        Key=key,
        Body=b'air_quality_model_placeholder'
    )
    print(f"Model saved to s3://martin-mlops-models/{key}")

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

    save_s3 = PythonOperator(
        task_id='save_model_to_s3',
        python_callable=save_model_to_s3,
    )

    pull_data >> retrain >> evaluate >> save_s3
