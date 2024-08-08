from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
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
    start_date=datetime(2024, 8, 1),
    catchup=True,
    tags=['helloworld'],
) as dag:

    start = EmptyOperator(
        task_id='start',
        )

    re_partition = EmptyOperator(
            task_id='re_partition'
            )

    join_df = BashOperator(
            task_id='join_df',
            bash_command='''
            echo "spark-submit....."
            echo "{{ds_nodash}}"
            ''',
            )

    agg_df = BashOperator(
            task_id='agg.df',
            bash_command='''
            echo "spark-submit....."
            echo "{{ds_nodash}}"
            ''',
            )

    task_end = EmptyOperator(
            task_id='task_end',
            )


    start >> re_partition >> join_df >> agg_df >> task_end
