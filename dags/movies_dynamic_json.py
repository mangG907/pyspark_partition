from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    BranchPythonOperator
)
import os

with DAG(
    'parsing',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_extract',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 17),
    end_date=datetime(2024, 8, 18),
    catchup=True,
    tags=['movie', 'extract'],
) as dag:


    get_data=EmptyOperator(
            task_id="re.partition",
        #python_callable=re_part,
        #requirements=["git+https://github.com/EstherCho-7/spark_flow.git@0.2.0/airflowdag"],
        #system_site_packages=False,
        #trigger_rule="one_success"
            )


    parsing_parquet=BashOperator(
        task_id="join.df",
        bash_command=""
            )


    select_parquet=BashOperator(
        task_id="agg.df",
        bash_command=""
            )

    start =EmptyOperator(
        task_id="start",
                )

    end = EmptyOperator(
        task_id="end",
                )
    

    start >> get_data
    get_data >> parsing_parquet >> select_parquet >> end 
    
