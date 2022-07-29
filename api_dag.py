import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
 def save_posts(ti) -> None:
    posts = ti.xcom_pull(task_ids=['get_posts'])
    with open('/Users/dradecic/airflow/data/posts.json', 'w') as f:
        json.dump(posts[0], f)
    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='NGC',
        endpoint='posts/'
    )

    # 2. Get the posts
    task_get_posts = SimpleHttpOperator( 
        task_id='get_posts',
        http_conn_id='NGC',
        endpoint='posts/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
        )
    
    # 3. Save the posts
    task_save = PythonOperator(
        task_id='save_posts',
        python_callable=save_posts
    )

    task_is_api_active >> task_get_posts >> task_save