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
import sys

with DAG(
    'movies-dynamic-json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_extract',
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 2),
    catchup=True,
    tags=['movie', 'extract'],
) as dag:

    def tmp():
        pass
    def get_data():
        from movdata.ml import save_movies
        save_movies(2015)

    # 파이썬가상오퍼레이터로 만들고
    # 데이터 받아오는 pip 모듈 설치 하고
    # 연결된 함수에서 받아서 저장 하기
    get_data=PythonVirtualenvOperator(
            task_id="get.data",
            python_callable=tmp,
            requirements=["git+https://github.com/mangG907/movdata.git@v2.0/dataframe"],
            system_site_packages=False,
            trigger_rule="one_success"
            )


    # 배시 오퍼레이터로
    # 스파크 서밋으로 파이스파크.py 만들어서
    # 이슈 68번의 샘플 코드를 보고 작업
    parsing_parquet=BashOperator(
        task_id="parsing.parquet",
        bash_command="""
        $SPARK_HOME/bin/spark-submit /home/manggee/code/sparksubmit/abc.py 2015
        """
        )


    select_parquet=BashOperator(
        task_id="select.parquet",
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
    
