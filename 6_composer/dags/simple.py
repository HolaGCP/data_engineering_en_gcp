"""An example DAG demonstrating simple Apache Airflow operators."""

from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 1, 1),
}

with models.DAG(
        'composer_sample_simple_greeting',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args,
        catchup=False) as dag:

    def greeting():
        import logging
        logging.info('Hello World!')

    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting
    )

    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.'
    )
    
    hello_python >> goodbye_bash
