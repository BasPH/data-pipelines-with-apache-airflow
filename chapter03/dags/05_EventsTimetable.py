from airflow import DAG
from airflow.timetables.events import EventsTimetable
from pendulum import datetime

scheduled_launches = EventsTimetable(
    event_dates=[
        datetime(2024, 1, 2),
        datetime(2024, 1, 7),
        datetime(2024, 1, 12),
    ]
)

with DAG(
    dag_id="05_query_with_events",
    schedule=scheduled_launches,
    start_date=datetime(2024, 1, 1),
):
    ...
