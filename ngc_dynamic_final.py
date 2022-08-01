from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta, date
from airflow.models import Variable
import docker

#  Define the default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),

}
# Read in variables from Airflow UI
v_list = Variable.get("v_list", deserialize_json=True)
username_docker = Variable.get("username_docker", deserialize_json=True)
password_docker = Variable.get("password_docker", deserialize_json=True)

#  Declare a DAG with default arguments
dag = DAG('dynamic_ngc_final',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False
          )

#  Declare dummy start and stop tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


#  Create dynamic tasks for each element of the list
def task(element):
    def start_container(**kwargs):
        # get the docker params from the environment
        client = docker.from_env()
        client.login(username=str(username_docker), password=str(password_docker))
        today=str(date.today())
        now =datetime.now()
        current_time = now.strftime("_%H-%M-%S")

        # run the container
        response = client.containers.run(
            # The container you wish to call
            'ngcclitest:20.04',
            # The commands to run inside the container
            '/project/ngc-cli/ngc batch run --name "airflow-' + str(username_docker)+ '_'+ today + current_time + str(element) + '" --preempt RUNONCE --min-timeslice 1s --total-runtime 0s --instance dgxa100.80g'+ str(element) + ' --commandline "cd /results; wget https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.17.3.tar.xz; tar -xvf linux-5.17.3.tar.xz; rm -rf /results/*" --result /results --image "nvidia/pytorch:22.04-py3"'
        )
    return start_container

for element in v_list:
    # Declare the task
    t1 = PythonOperator(
            task_id='ngc_batch_run' + str(element),
            python_callable=task(element),
            dag=dag      
    )

    # Define the sequence of execution of tasks
    start_task >> t1  >> end_task
