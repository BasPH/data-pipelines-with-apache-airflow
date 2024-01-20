import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="listing_4_01",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@hourly",
    max_active_runs=1,
):
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ data_interval_start.year }}/"
            "{{ data_interval_start.year }}-{{ '{:02}'.format(data_interval_start.month) }}/"
            "pageviews-{{ data_interval_start.year }}"
            "{{ '{:02}'.format(data_interval_start.month) }}"
            "{{ '{:02}'.format(data_interval_start.day) }}-"
            "{{ '{:02}'.format(data_interval_start.hour) }}0000.gz"
        ),
    )
