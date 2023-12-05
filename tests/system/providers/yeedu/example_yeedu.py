from airflow import DAG
from datetime import datetime, timedelta
from providers.yeedu.operators.yeedu import YeeduJobRunOperator

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'pravallika',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'dag_system_test',
    default_args=default_args,
    description='DAG to submit Spark job to Yeedu',
    schedule_interval='@once',  # You can modify the schedule_interval as needed
)

# YeeduOperator task to submit the Spark job
yeedu_task = YeeduJobRunOperator(
    task_id='dag_system_test',
    job_conf_id=1,
    token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxOSwiaWF0IjoxNzAxNDMxOTE1LCJleHAiOjE3MDE2MDQ3MTV9.1KC23awRYx1X8lZwRY9BlP-As3oNp0aefo6d2x7X7oU',
    hostname='10.128.0.60:8080',
    workspace_id=3,
    dag=dag,
)

from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)