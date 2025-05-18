from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define our task1
def preprocess_data():
    print("Preprocessing the data...")

## definr our task 2
def train_model():
    print("Model Training...")

## Define our task 3
def evaluate_model():
    print("Model Evaluate...")

## Define the DAG

with DAG(
    'ml_pipeline',
    start_date = datetime(2025,5,18),
    schedule = '@weekly'
) as dag:
    
    ## Define the task
    preprocess = PythonOperator(task_id='preprocess_task', python_callable=preprocess_data)
    train = PythonOperator(task_id='train_task', python_callable=train_model)
    evaluate = PythonOperator(task_id='evaluate_task', python_callable=evaluate_model)

    ## Set Dependencies

    preprocess >> train >> evaluate