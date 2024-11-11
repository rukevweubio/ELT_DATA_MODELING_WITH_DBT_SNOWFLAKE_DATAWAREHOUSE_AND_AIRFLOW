from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Define the DAG
with DAG(
    'dummy_task_dag',
    description='A simple DAG with a dummy task',
    schedule_interval=None,  # No schedule; run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Define the dummy task
    dummy_task = DummyOperator(
        task_id='dummy_task',
    )


     dummy_task2 = DummyOperator(
        task_id='dummy_task2',
    )

# In this simple DAG, dummy_task is the only task and will run by itself
dummy_task>>dummy_task2