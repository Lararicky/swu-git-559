from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def change_datatype():
    df = pd.read_csv('/tmp/tested.csv') 
                
    convert_dict = {'Pclass': float}
    df = df.astype(convert_dict)

    df.to_parquet('/tmp/tested_2.csv') 

def filter_male():
    df = pd.read_parquet('/tmp/tested_2.csv')
    mask = df['Sex'] == 'male'
    print(mask)
    filter_row_df = df[mask]
    query_row_df = df.query('Sex == "male"')
    print(query_row_df)

    df.to_parquet('/tmp/filterResult.csv') 

default_args = {
    "owner": "airflow", 
    "depends_on_past": False,
    "email" : ['airflow@example.com'],
}    

with DAG(
    'dataframe_lab1', 
    default_args=default_args, 
    schedule_interval=timedelta(days=1), 
    start_date=days_ago(2), 
    tags=['airflow tab'],
)as dag:
    start = DummyOperator( 
        task_id="start"
    )  

    end = DummyOperator( 
        task_id="end"
    )
    
    change_datatype_task = PythonOperator( 
        task_id='change_datatype', 
        python_callable=change_datatype
    )

    filter_Male = PythonOperator( 
        task_id='sfilter_male', 
        python_callable=filter_male
    )

start >> change_datatype_task >> filter_Male  >> end