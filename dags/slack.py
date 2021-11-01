
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'

def slack_failed_callback(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)

args = {
    'owner': 'airflow',
    'on_failure_callback': slack_failed_callback
}

with DAG(
    dag_id='slack',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['slack']
) as dag:

    entrypoint = DummyOperator(
        task_id='entrypoint',
    )

    bash = BashOperator(
        task_id='fail_task',
        bash_command='exit 1'
        # on_failure_callback=slack_failed_callback,
        # provide_context=True
    )

    endpoint = DummyOperator(
        task_id='endpoint',
    )

    entrypoint >> bash >> endpoint

if __name__ == "__main__":
    dag.cli()