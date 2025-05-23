"""
We'll define a DAG where the tasks are as follows:

Task 1: Start with an initial number (e.g., 10).
Task 2: Add 5 to the number.
Task 3: Multiply the result by 2.
Task 4: Subtract 3 from the result.
Task 5: Compute the square of the result.    
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Defining the function for each task
def start_number(**context):
    context["ti"].xcom_push(key='current_value', value=10)
    print("Starting number 10.")

def add_five(**context):
    current_val = context['ti'].xcom_pull(key='current_value', task_ids='start_task')
    new_value = current_val + 5
    context["ti"].xcom_push(key='current_value', value=new_value)
    print(f"Add 5 : {current_val} + 5 = {new_value}")

def multiply_by_two(**context):
    current_val = context['ti'].xcom_pull(key='current_value', task_ids='add_five_task')
    new_value = current_val * 2
    context["ti"].xcom_push(key='current_value', value=new_value)
    print(f"Multiply 2 : {current_val} * 2 = {new_value}")

def subtract_three(**context):
    current_val = context['ti'].xcom_pull(key='current_value', task_ids='Multiply_two_task')
    new_value = current_val - 3
    context["ti"].xcom_push(key='current_value', value=new_value)
    print(f"Subtract 3 : {current_val} - 3 = {new_value}")

def result_square(**context):
    current_val = context['ti'].xcom_pull(key='current_value', task_ids='subtract_three_task')
    new_value = current_val ** 2
    context["ti"].xcom_push(key='current_value', value=new_value)
    print(f"Result Square : {current_val} ** 2 = {new_value}")

# Define DAG
with DAG(
    dag_id="math_sequence_dag",
    start_date=datetime(2025, 5, 18),
    schedule='@once',
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_number,
    )

    add_five_task = PythonOperator(
        task_id='add_five_task',
        python_callable=add_five,
    )

    multiply_by_2 = PythonOperator(
        task_id='Multiply_two_task',
        python_callable=multiply_by_two,
    )

    subtract_3 = PythonOperator(
        task_id='subtract_three_task',
        python_callable=subtract_three,
    )

    square_task = PythonOperator(
        task_id='square_number_task',
        python_callable=result_square,
    )

    # Task Dependencies
    start_task >> add_five_task >> multiply_by_2 >> subtract_3 >> square_task
