import airflow
import psycopg2
import unittest
from datetime import timedelta
from airflow import DAG
from airflow.models import DagBag
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import TaskInstance
from airflow.operators import DbtCloudRunJobOperator
from extract_load_outfittery import load_posts
from extract_load_outfittery import load_posthistory
from extract_load_outfittery import load_votes
from extract_load_outfittery import load_users
from extract_load_outfittery import load_postlinks
from extract_load_outfittery import load_comments
from extract_load_outfittery import load_badges

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['arsalan-93@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0}

dag = DAG('arsalan_outfittery_challenge', default_args=default_args, description='Build ELT for Outfittery Challenge', \
          schedule_interval=None) # Schedule can be set to anytime desired

t1 = PythonOperator(
    task_id='load_badges',
    provide_context=True,
    python_callable=load_badges,
    dag=dag)

t2 = PythonOperator(
    task_id='load_comments',
    provide_context=True,
    python_callable=load_comments,
    dag=dag)

t3 = PythonOperator(
    task_id='load_posts',
    provide_context=True,
    python_callable=load_posts,
    dag=dag)

t4 = PythonOperator(
    task_id='load_postlinks',
    provide_context=True,
    python_callable=load_postlinks,
    dag=dag)

t5 = PythonOperator(
    task_id='load_users',
    provide_context=True,
    python_callable=load_users,
    dag=dag)

t6 = PythonOperator(
    task_id='load_votes',
    provide_context=True,
    python_callable=load_votes,
    dag=dag)

t7 = PythonOperator(
    task_id='load_posthistory',
    provide_context=True,
    python_callable=load_posthistory,
    dag=dag)

# Run dbt job which has to be setup on dbt cloud.
# Have not tested it's functionality yet
t8 = DbtCloudRunJobOperator(
    task_id='run_dbt_cloud_job',
    dbt_cloud_conn_id='dbt_cloud',
    job_name='Outfittery Job',
    dag=dag)

"""
Verify that data source files describing DAGs load without breaching the threshold
"""
def test_dagbag_import_time(self):
    stats = self.dagbag.dagbag_stats
    slow_files = filter(lambda d: d.duration > self.LOAD_SECOND_THRESHOLD, stats)
    res = ', '.join(map(lambda d: d.file[1:], slow_files))

    self.assertEquals(
        0,
        len(slow_files),
        'The following files take more than {threshold}s to load: {res}'.format(
            threshold=self.LOAD_SECOND_THRESHOLD,
            res=res
        )
    )

# Built this test function to test airflow within the code.
def test_t1():
    dag_id = 'arsalan_outfittery_challenge'
    dag = DagBag().get_dag(dag_id)
    t1_task = dag.get_task('t1')
    execution_date = datetime.now()
    t1_ti = TaskInstance(task=t1_task, execution_date=execution_date)
    context = t1_ti.get_template_context()
    t1_task.execute(context)

# if __name__ == '__main__':
#     test_t1()

t8 >> t1, t2, t3, t4, t5, t6, t7