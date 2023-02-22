import pendulum

from datetime import datetime
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context



@task
def run_this_func(dag_run=None):
    """
    Print the payload "message" passed to the DagRun conf attribute.

    :param dag_run: The DagRun object
    """
    a = [1,2,3,4,5]
    context = get_current_context()

    for i in a:
        TriggerDagRunOperator(
            task_id="trigger_dagrun",
            trigger_dag_id="target_dag",  # Ensure this equals the dag_id of the DAG to trigger,
            conf={"message": i}
        ).execute(context)



with DAG(
    dag_id="trigger_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:

    run_this = run_this_func()
