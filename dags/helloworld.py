from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.models import Variable
from pprint import pprint

with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 5),
    catchup=True,
    tags=['helloworld'],
) as dag:

    start = EmptyOperator(
        task_id='start',
        )

    def rm_dir(dir_path):
        import os
        import shutil
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

    def re_partition(ds_nodash):
        import os
        import pandas as pd

        load_dt = ds_nodash
        home_dir = os.path.expanduser("~")
        from_path='/data/movie/extract'
        # /home/manggee/data/movie/extract
        read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
        write_base = f'{home_dir}/data/movie/repartition'
        write_path = f'{write_base}/load_dt={load_dt}'

        df = pd.read_parquet(read_path)
        df['load_dt'] = load_dt
        rm_dir(write_path)
        df.to_parquet(
            write_path,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )
        print("*" * 99)


    re_task = PythonOperator(
            task_id='re.partition',
            python_callable=re_partition,
            )

    join_df = BashOperator(
            task_id='join_df',
            bash_command='''
            echo "spark-submit....."
            $SPARK_HOME/bin/spark-submit /home/manggee/code/spark_airs/dags/SimpleApp.py "APPNAME" {{ ds_nodash }}
            ''',
            )

    agg_df = BashOperator(
            task_id='agg.df',
            bash_command='''
            echo "spark-submit....."
            $SPARK_HOME/bin/spark-submit /home/manggee/code/spark_airs/dags/SimpleAgg.py {{ ds_nodash }}
            ''',
            )

    task_end = EmptyOperator(
            task_id='task_end',
            )


    start >> re_task >> join_df >> agg_df >> task_end
