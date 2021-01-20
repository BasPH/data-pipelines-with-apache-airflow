import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="chapter12_task_failure_email",
    default_args={"email": "bob@work.com"},
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)

failing_task = BashOperator(task_id="failing_task", bash_command="exit 1", dag=dag)

# export AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
# export AIRFLOW__SMTP__SMTP_MAIL_FROM=bob@gmail.com
# export AIRFLOW__SMTP__SMTP_PASSWORD=123456789abcdefg
# export AIRFLOW__SMTP__SMTP_PORT=587
# export AIRFLOW__SMTP__SMTP_SSL=False
# export AIRFLOW__SMTP__SMTP_STARTTLS=True
# export AIRFLOW__SMTP__SMTP_USER=bob@gmail.com
