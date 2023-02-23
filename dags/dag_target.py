import pendulum
from datetime import timedelta
from time import sleep
import random

from datetime import datetime
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.bash import BashOperator


@task
def print_input(x):
    sleep(5)
    if random.choice(range(2)) == 1:
        raise Exception("random failure")
    print(x)


args = {
    "owner": "jmtaysom",
    "retries": 5,
    "retry_delay": timedelta(seconds=2),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="target_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    default_args=args,
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    #  bash_task = BashOperator(
    #     task_id="bash_task",
    #     bash_command='echo "Here is the message: $message"',
    #     env={"message": '{{ dag_run.conf.get("message") }}'},
    # )
    print_input('{{ dag_run.conf.get("message") }}')
