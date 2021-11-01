
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
    dag_id='realestate',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['realestate']
) as dag:

    entrypoint = DummyOperator(
        task_id='entrypoint',
    )

    apt_trade_service_key1 = BashOperator(
        task_id='apt-trade-service-key1',
        depends_on_past=False,
        bash_command='cd /forkrane; python app.py --url_type apt-trade --service_key service_key1' ,
        retries=3,
    )
    apt_trade_service_key2 = BashOperator(
        task_id='apt-trade-service-key2',
        depends_on_past=False,
        bash_command='cd /forkrane; python app.py --url_type apt-trade --service_key service_key2' ,
        retries=3,
    )

    apt_rent_service_key1 = BashOperator(
        task_id='apt-rent-service-key1',
        depends_on_past=False,
        bash_command='cd /forkrane; python app.py --url_type apt-rent --service_key service_key1' ,
        retries=3,
    )
    apt_rent_service_key2 = BashOperator(
        task_id='apt-rent-service-key2',
        depends_on_past=False,
        bash_command='cd /forkrane; python app.py --url_type apt-rent --service_key service_key2' ,
        retries=3,
    )

    endpoint = DummyOperator(
        task_id='endpoint',
    )

    entrypoint >> [apt_rent_service_key1, apt_trade_service_key1]
    apt_rent_service_key1 >> apt_rent_service_key2 >> endpoint
    apt_trade_service_key1 >> apt_trade_service_key2 >> endpoint

if __name__ == "__main__":
    dag.cli()