from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone

default_args = {
    "owner": "movies_docker",
    "depends_on_past": False,
    "email": ["k7ragav@gmail.com"],
    "email_on_failure": True,
    "email_on_success": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": True,
}
intervals = {
    "daily_at_8am": "0 8 */1 * *",
    "daily_at_7am": "0 7 */1 * *",
    "every_3_days": "0 0 */3 * *",
    "weekly_monday_8pm": "0 18 * * 1",
    "weekly_tuesday_8pm": "0 18 * * 2",
    "weekly_wednesday_8pm": "0 12 * * 3",
}
bash_command = "docker exec movies_docker python {{ task.task_id }}.py "
bash_command_with_date = "docker exec movies_docker python {{ task.task_id }}.py {{ds}}"

with DAG(
        "netflix_top10",
        description="netflix top 10",
        default_args=default_args,
        schedule_interval=intervals["weekly_wednesday_8pm"],
        start_date=datetime(2022, 8, 2, tzinfo=timezone("Europe/Amsterdam")),
) as netflix_top10_dag:
    netflix_top10_task = BashOperator(
        task_id="netflix_top10",
        bash_command=bash_command,
    )

with DAG(
        "box_office_numbers_weekly",
        description="boxoffice_numbers_weekly",
        default_args=default_args,
        schedule_interval=intervals["weekly_tuesday_8pm"],
        start_date=datetime(2021, 12, 21, tzinfo=timezone("Europe/Amsterdam")),
) as box_office_numbers_weekly_dag:
    numbers_weekly_extract_task = BashOperator(
        task_id="thenumbers_weekly_extract",
        bash_command=bash_command_with_date,
    )
    numbers_weekly_load_task = BashOperator(
        task_id="thenumbers_weekly_load",
        bash_command=bash_command_with_date,
    )
    numbers_weekly_extract_task >> numbers_weekly_load_task