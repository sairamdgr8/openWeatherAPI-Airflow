from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
import configparser
import os


path = os.environ['AIRFLOW_HOME']


print("airflow_path***********",path)


args = {
    'owner': 'sairamdgr8',
    'start_date': days_ago(1), # make start date in the past
    'email': ['sairamdgr8.airflow@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='openApiWeatherdag',
    default_args=args,
    schedule_interval='30 21 * * *' # every day 21:30 HH:MM
)

t1 = BashOperator(
    task_id='start',
    bash_command='echo "***************  code start   **************"',
    dag=dag)

t2 = BashOperator( task_id='get_weather_api_code',
                    bash_command=f'python {path}/dags/src/openApiWeather.py',
                    dag=dag)
                    
t3 = BashOperator(
    task_id='end',
    bash_command='echo "***************  code end   **************"',
    dag=dag)
    
    
t1>> t2>>t3
