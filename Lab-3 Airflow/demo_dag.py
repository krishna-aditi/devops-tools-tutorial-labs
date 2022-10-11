# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 17:13:50 2022

@author: krish
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_example():
    return 'Starting Lab-3 execution!'

def hello_world():
    return 'Hello World!'

def print_status():
    print('Successfully run Lab-3 Tutorial example')
    return 'Successfully run Lab-3 Tutorial example'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 1),
    'email_on_failure': False,
    'retries': 2,
    'max_active_runs': 1
}

dag = DAG('example_dag', default_args=default_args, schedule_interval='@daily')
dag.doc_md = """
#### DAG Summary as a markdown
This is an example dag for lab 3-Airflow.
"""

start = PythonOperator(task_id='start', python_callable = start_example, dag=dag)
start.doc_md = """ #start operator """

hello = PythonOperator(task_id='hello', python_callable = hello_world, dag=dag)
hello.doc_md = """ #hello world operator """

print_task = PythonOperator(task_id='print', python_callable = print_status, dag=dag)
print_task.doc_md = """ #print status operator """


# DAG
start >> hello >> print_task
