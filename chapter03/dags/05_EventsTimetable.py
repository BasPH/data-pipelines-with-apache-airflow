import pendulum

from airflow import DAG
from airflow.timetables.events import EventsTimetable

scheduled_launches = EventsTimetable(
        event_dates=[
            pendulum.datetime(2024, 1, 2),
            pendulum.datetime(2024, 1, 7),
            pendulum.datetime(2024, 1, 12),
        ]
    )

with DAG(
    dag_id="05_query_with_events",
    schedule=scheduled_launches,
    start_date=pendulum.datetime(2024,1,1),
):
    ...

