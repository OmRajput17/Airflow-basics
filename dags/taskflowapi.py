from airflow import DAG
from airflow.decorators import task
from datetime import datetime


with DAG(
    dag_id = 'math_sequence_with_Taskflow',
    start_date = datetime(2025,5,18),
    schedule = '@once',
    catchup = False,
) as dag:
    
    ## Defining task

    # Task 1 : Start with the initial number
    @task
    def start_number():
        initial_value = 10
        print(f"Initial Number is {initial_value}")
        return initial_value
    
    # Task 2 : Add 5 to the number
    @task
    def add_five(number):
        new_value = number + 5
        print(f"I{number} + 5 = {new_value}")
        return new_value

    #task 3 : Multiply it with 2
    @task
    def multiply_by_two(number):
        new_value = number * 2
        print(f"{number} * 2 = {new_value}")
        return new_value
    
    # tAsk 4 : Subtract 3
    @task
    def subtract_three(number):
        new_value = number - 3
        print(f"{number}- 3 = {new_value}")
        return new_value
    
    ## tAsk 5 : Square of the result
    @task
    def square(number):
        new_value = number**2
        print(f"{number} ** 2 = {new_value}")
        return new_value
    
    ## Set the task dependencies
    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_by_two(added_value)
    subtracted_value = subtract_three(multiplied_value)
    square_value = square(subtracted_value)