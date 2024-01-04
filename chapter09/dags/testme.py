import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(dag_id="testme", start_date=pendulum.today("UTC").add(days=-3), schedule_interval=None)

t1 = DummyOperator(task_id="test", dag=dag)
for tasknr in range(5):
    BashOperator(task_id="test2", bash_command=f"echo '{tasknr}'", dag=dag) >> t1
