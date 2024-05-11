from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowFailException
import notifier

def failTask():
    raise AirflowFailException("force fail.")



with DAG(
    dag_id = "notifier",
    description='通知功能測試',
    start_date = None,
    schedule_interval=None
    
) as dag:
    success_task = DummyOperator(
        task_id="success_task",
        on_success_callback=notifier.SuccessNotifier(message="Success Message")
    )

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=failTask,
        on_failure_callback=notifier.FailNotifier(message="Fail Message")
    )

    success_task >> fail_task