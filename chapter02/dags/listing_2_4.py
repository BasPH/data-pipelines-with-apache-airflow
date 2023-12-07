import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with  DAG(
    dag_id="listing_2_03",
    start_date=pendulum.today("UTC").add(days=-14),
    schedule=None,
):

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    )
