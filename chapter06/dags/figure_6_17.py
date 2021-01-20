import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ================================================ EXAMPLE 1 =================================================

example_1_dag_1 = DAG(
    dag_id="figure_6_17_example_1_dag_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_1_dag_2 = DAG(
    dag_id="figure_6_17_example_1_dag_2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

DummyOperator(task_id="etl", dag=example_1_dag_1) >> TriggerDagRunOperator(
    task_id="trigger_dag2",
    trigger_dag_id="figure_6_17_example_1_dag_2",
    dag=example_1_dag_1,
)
PythonOperator(
    task_id="report", dag=example_1_dag_2, python_callable=lambda: print("hello")
)

# ================================================ EXAMPLE 2 =================================================

example_2_dag_1 = DAG(
    dag_id="figure_6_17_example_2_dag_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_2_dag_2 = DAG(
    dag_id="figure_6_17_example_2_dag_2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_2_dag_3 = DAG(
    dag_id="figure_6_17_example_2_dag_3",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_2_dag_4 = DAG(
    dag_id="figure_6_17_example_2_dag_4",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

for dag_ in [example_2_dag_1, example_2_dag_2, example_2_dag_3]:
    DummyOperator(task_id="etl", dag=dag_) >> TriggerDagRunOperator(
        task_id="trigger_dag4", trigger_dag_id="figure_6_17_example_2_dag_4", dag=dag_
    )

PythonOperator(
    task_id="report", dag=example_2_dag_4, python_callable=lambda: print("hello")
)

# ================================================ EXAMPLE 3 =================================================

example_3_dag_1 = DAG(
    dag_id="figure_6_17_example_3_dag_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_3_dag_2 = DAG(
    dag_id="figure_6_17_example_3_dag_2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)
example_3_dag_3 = DAG(
    dag_id="figure_6_17_example_3_dag_3",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)
example_3_dag_4 = DAG(
    dag_id="figure_6_17_example_3_dag_4",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

DummyOperator(task_id="etl", dag=example_3_dag_1) >> [
    TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="figure_6_17_example_3_dag_2",
        dag=example_3_dag_1,
    ),
    TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="figure_6_17_example_3_dag_3",
        dag=example_3_dag_1,
    ),
    TriggerDagRunOperator(
        task_id="trigger_dag4",
        trigger_dag_id="figure_6_17_example_3_dag_4",
        dag=example_3_dag_1,
    ),
]
PythonOperator(
    task_id="report", dag=example_3_dag_2, python_callable=lambda: print("hello")
)
PythonOperator(
    task_id="report", dag=example_3_dag_3, python_callable=lambda: print("hello")
)
PythonOperator(
    task_id="report", dag=example_3_dag_4, python_callable=lambda: print("hello")
)
