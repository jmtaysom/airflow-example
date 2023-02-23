import pendulum

from datetime import datetime
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor


@task
def run_this_func(dag_run=None):
    """
    Print the payload "message" passed to the DagRun conf attribute.

    :param dag_run: The DagRun object
    """
    context = get_current_context()

    for i in range(10):
        TriggerDagRunOperator(
            task_id="trigger_dagrun",
            trigger_dag_id="target_dag",  # Ensure this equals the dag_id of the DAG to trigger,
            conf={"message": i},
        ).execute(context)


with DAG(
    dag_id="trigger_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    external_task_sensor = ExternalTaskSensor(
    task_id='external_task_sensor',
    poke_interval=60,
    timeout=5,
    soft_fail=False,
    retries=0,
    failed_states=['queued'],
    allowed_states=[
        'failed',
        'restarting',
        'running', 
        'scheduled',
        'shutdown', 
        'skipped', 
        'success', 
        'up_for_reschedule', 
        'up_for_retry',
    ],
    external_task_id='print_input',
    external_dag_id='target_dag',
    dag=dag)


    run_this = run_this_func()
    external_task_sensor >> run_this
