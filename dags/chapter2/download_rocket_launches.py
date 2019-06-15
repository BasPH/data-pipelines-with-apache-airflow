import json
import pathlib

import airflow
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches", start_date=airflow.utils.dates.days_ago(14)
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json 'https://launchlibrary.net/1.4/launch?next=5&mode=verbose'",
    dag=dag,
)


def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["rocket"]["imageURL"] for launch in launches["launches"]]
        for image_url in image_urls:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"
            open(target_file, "wb").write(response.content)
            print(f"Downloaded {image_url} to {target_file}")


get_pictures = PythonOperator(
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls -f /tmp/images/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
