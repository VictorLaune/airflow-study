from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

default_args = {
    'retry':5,
    'retry_delay':timedelta(minutes=5)
}

def _downloading_data(ti, **kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')
    # We can pass only 'ds' like parameter to receive only de execution date.
    # print(ds)

    # return 42 to xcom
    ti.xcom_push(key='my_key', value=42)

def _checking_data(ti):
    my_xcom = ti.xcom_pull(key='return_value', task_ids=['downloading_data'])
    print(my_xcom)


with DAG(dag_id='simple_dag', default_args=default_args, schedule_interval="@daily", 
         start_date=days_ago(3), catchup=False, max_active_runs=3) as dag:
    """
    - dag_id is the unique identifier of your DAG
    
    - start_date is used with datetime. If you specify the start date
      directly in the DAG object, this start date is applied to all of your operators. 
    
    - By default, the datetime is in UTC.
    
    - By default, Airflow will run all the non trigger DAG runs between the current date and the start date.
    
    - By default, your DAGS are scheduled to runs every 24h
    """


    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        poke_interval=15
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0' 
    )

    chain(downloading_data, checking_data, [waiting_for_data, processing_data])