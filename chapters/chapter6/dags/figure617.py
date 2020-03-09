import airflow.utils.dates
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import State

dag11 = DAG(
    dag_id="chapter6_figure617_dag11",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag12 = DAG(
    dag_id="chapter6_figure617_dag12",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

DummyOperator(task_id="etl", dag=dag11) >> TriggerDagRunOperator(
    task_id="trigger_dag2", trigger_dag_id="chapter6_figure617_dag12", dag=dag11
)
PythonOperator(task_id="report", dag=dag12, python_callable=lambda: print("hello"))

# ============================================================================================================

dag21 = DAG(
    dag_id="chapter6_figure617_dag21",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag22 = DAG(
    dag_id="chapter6_figure617_dag22",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)
dag23 = DAG(
    dag_id="chapter6_figure617_dag23",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)
dag24 = DAG(
    dag_id="chapter6_figure617_dag24",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

DummyOperator(task_id="etl", dag=dag21) >> [
    TriggerDagRunOperator(
        task_id="trigger_dag2", trigger_dag_id="chapter6_figure617_dag22", dag=dag21
    ),
    TriggerDagRunOperator(
        task_id="trigger_dag3", trigger_dag_id="chapter6_figure617_dag23", dag=dag21
    ),
    TriggerDagRunOperator(
        task_id="trigger_dag4", trigger_dag_id="chapter6_figure617_dag24", dag=dag21
    ),
]
PythonOperator(task_id="report", dag=dag22, python_callable=lambda: print("hello"))
PythonOperator(task_id="report", dag=dag23, python_callable=lambda: print("hello"))
PythonOperator(task_id="report", dag=dag24, python_callable=lambda: print("hello"))

# ============================================================================================================

dag31 = DAG(
    dag_id="chapter6_figure617_dag31",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag32 = DAG(
    dag_id="chapter6_figure617_dag32",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag33 = DAG(
    dag_id="chapter6_figure617_dag33",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag34 = DAG(
    dag_id="chapter6_figure617_dag34",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

DummyOperator(task_id="etl", dag=dag31)
DummyOperator(task_id="etl", dag=dag32)
DummyOperator(task_id="etl", dag=dag33)
[
    ExternalTaskSensor(
        task_id="wait_for_etl_dag1",
        external_dag_id="chapter6_figure617_dag31",
        external_task_id="etl",
        dag=dag34,
        allowed_states=State.task_states,
    ),
    ExternalTaskSensor(
        task_id="wait_for_etl_dag2",
        external_dag_id="chapter6_figure617_dag32",
        external_task_id="etl",
        dag=dag34,
        allowed_states=State.task_states,
    ),
    ExternalTaskSensor(
        task_id="wait_for_etl_dag3",
        external_dag_id="chapter6_figure617_dag33",
        external_task_id="etl",
        dag=dag34,
        allowed_states=State.task_states,
    ),
] >> PythonOperator(task_id="report", dag=dag34, python_callable=lambda: print("hello"))
