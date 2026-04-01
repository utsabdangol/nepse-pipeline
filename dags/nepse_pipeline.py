#In Airflow, a DAG represents a data pipeline or workflow with a start and an end
# dag hold all the action when to execute ,rules to execute and what to execute
from airflow import DAG
#an operator is a task it take python function and execute it in a task
from airflow.operators.python import PythonOperator
#used to run bash commands in a task
from airflow.operators.bash import BashOperator
#timedale is used to define time intervals for scheduling and retrying tasks
from datetime import datetime, timedelta
import sys
import os

# add src to path so airflow can find our modules
sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'utsab',
    # how many times to retry if the task fails
    'retries': 1,
    # interval between retries
    'retry_delay': timedelta(minutes=5),
    # if a task fails, do not send email notifications
    # in actual production, you might want to set this to True and configure email settings
    'email_on_failure': False,
}

def run_scraper():
    # imports the scraper and db modules from our src directory
    # these imports are inside the function to avoid issues with Airflow's 
    # DAG parsing and to ensure they are only imported when the task runs
    from ingestion.scraper import fetch_nepse_data
    from ingestion.scraper import trading_date
    from ingestion.db import create_table, save_to_postgres
    from datetime import date

    print("Starting NEPSE scrape...")
    # when calling the this funcation it will not run the scraper.py itself 
    # but it will run the fetch_nepse_data function inside scraper.py and return a dataframe
    # this is why we have to import the funtion of db.py as well
    df = fetch_nepse_data()

    if df is None:
        raise ValueError("Scraper returned no data")

    filename = f"/opt/airflow/src/data/raw/nepse_{trading_date}.csv"
    #makes sure the directory exists before trying to save the file
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"Saved CSV to {filename}")

    create_table()
    save_to_postgres(df)
    print(f"Saved {len(df)} rows to PostgreSQL")

# defines the DAG and its tasks
with DAG(

    dag_id='nepse_daily_pipeline',
    default_args=default_args,
    description='Daily NEPSE data pipeline',
    # schedule_interval defines when the DAG should run.
    # 0= minute, 11=hour, * = every day, * = everymonth, 1-5 = Monday to Friday
    # Nepal is UTC+5:45, so 4pm Nepal = 10:15am UTC from sunday to thursday

    schedule_interval='15 12 * * 0,1,2,3,4',
    start_date=datetime(2026, 3, 31),
    # is catchup were to be true it would try to run all the missed runs from start-date to today
    catchup=False,
    tags=['nepse', 'production'],
) as dag:
    # defines the first task which is to run the scraper and save data to postgres
    # in airflow pythonoerator is a class that allows you to run a python function as a task in your DAG
    scrape_task = PythonOperator(
        task_id='scrape_nepse',
        python_callable=run_scraper,
    )

    # defines the second task which is to run dbt models and tests
    # in airflow bashoperator is a class that allows you to run bash commands as a task in your DAG
    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='docker exec nepse_dbt bash -c "cd /usr/app/dbt/nepse && dbt run"',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='docker exec nepse_dbt bash -c "cd /usr/app/dbt/nepse && dbt test"',
    )
    # this defines the order of execution for the tasks in the DAG
    # also defines the dependencies between tasks, ensuring that the scraper runs before the dbt tasks
    scrape_task >> dbt_run_task >> dbt_test_task

    #wtiout this dependance chain all three task would run at the same time and 
    # dbt tasks would fail because they depend on the data being in postgres which is done by the scraper task
    # or it would show false positive using the old data