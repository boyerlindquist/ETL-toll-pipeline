from datetime import timedelta
from datetime import datetime
import pendulum

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define default argument
default_args = {
    'owner': 'Imam',
    'start_date': days_ago(0),
    'email': ['imammamqs@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1)
)

# unzip tgz file to finalassignment directory
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/imam/airflow/dags/finalassignment/tolldata.tgz -C /home/imam/airflow/dags/finalassignment',
    dag=dag
)

# extracting data from csv
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command='cut -f1-4 -d"," /home/imam/airflow/dags/finalassignment/vehicle-data.csv > /home/imam/airflow/dags/finalassignment/csv_data.csv',
    dag=dag
)

# extracting data from tsv
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command="cut -f5-7 -d$'\t' /home/imam/airflow/dags/finalassignment/tollplaza-data.tsv | tr '\t' ','> /home/imam/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag
)

# extracting data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command='cut -c59-67 /home/imam/airflow/dags/finalassignment/payment-data.txt | tr " " ","  > /home/imam/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag
)

# consolidating data
consolidate_data = BashOperator(
    task_id ='consolidate_data',
    bash_command='paste -d, /home/imam/airflow/dags/finalassignment/csv_data.csv \
        /home/imam/airflow/dags/finalassignment/tsv_data.csv\
        /home/imam/airflow/dags/finalassignment/fixed_width_data.csv\
        > /home/imam/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag
)

# transforming data
transform_data= BashOperator(
    task_id='transform_data',
    bash_command="tr 'a-z' 'A-Z' < /home/imam/airflow/dags/finalassignment/extracted_data.csv > /home/imam/airflow/dags/finalassignment/transformed_data.csv",
    dag=dag
)

# defining the pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data