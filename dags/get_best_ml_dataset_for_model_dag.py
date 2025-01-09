from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from random import randint
from airflow.operators.bash import BashOperator

def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
        ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

def training_model():
    return randint(1,10)


with DAG(
    "get_best_ml_dataset_for_model_dag",
    start_date = datetime(2025, 1, 1),
    schedule = "@daily",
    catchup=False
    ) as dag:

    training_model_A = PythonOperator(
        task_id = "training_model_A",
        python_callable = training_model
    )

    training_model_B = PythonOperator(
        task_id = "training_model_B",
        python_callable = training_model
    )

    training_model_C = PythonOperator(
        task_id = "training_model_C",
        python_callable = training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id = "choose_best_model",
        python_callable = choose_best_model
    )

    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo 'accurate'",
    )

    accurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo 'inaccurate'",
    )