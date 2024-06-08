from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# Define default arguments for the DAG
default_args = {
    'owner': 'oussama',
    'start_date': days_ago(0),
    'email':['khatt.ouss.02@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay':timedelta(minutes=5)
}

# Create an instance of the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Set your desired schedule interval or None for manual triggering
    description='Apache Airflow Final Assignment',
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -zxvf /home/project/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv-data.csv',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv-data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_widt',
    bash_command='''cut -c59-61 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > col1.txt
    cut -c63-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > col2.txt
    paste -d',' col1.txt col2.txt >fixed_width_data.csv''',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='''paste /home/project/airflow/dags/finalassignment/staging/csv-data.csv /home/project/airflow/dags/finalassignment/staging/tsv-data.csv > 1.csv 
    paste 1.csv fixed_width_data.csv''',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr [:lower:] [:upper:] < /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > transformed_data.csv ',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data