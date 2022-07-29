import os, json, base64, requests
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import docker

with DAG(
    "API_CALL_WORKSPACE", 
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1), 
    catchup=False
   
) as dag:

    @task(task_id='api_connect')
    def find_api_key():
        expanded_conf_file_path = os.path.expanduser("~/.ngc/config")
        if os.path.exists(expanded_conf_file_path):
            print("Config file exists, pulling API key from it")
            try:
                config_file = open(expanded_conf_file_path, "r")
                lines = config_file.readlines()
                for line in lines:
                 if "apikey" in line:
                    elements = line.split()
                    return elements[-1]
            except:
                print("Failed to find the API key in config file")
                return ''
        elif os.environ.get('API_KEY'):
            print("Using API_KEY environment variable")
            return os.environ.get('API_KEY')
        else:
            print("Could not find a valid API key")
            return ''

    @task(task_id='token')
    def get_token(org, team):
        '''Use the api key set environment variable to generate auth token'''
        scope = f'group/ngc:{org}'
        if team: #shortens the token if included
           scope += f'/{team}'
        querystring = {"service": "ngc", "scope": scope}
        auth = '$oauthtoken:{0}'.format(find_api_key())
        auth = base64.b64encode(auth.encode('utf-8')).decode('utf-8')
        headers = {
            'Authorization': f'Basic {auth}',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
         }
        url = 'https://authn.nvidia.com/token'
        response = requests.request("GET", url, headers=headers, params=querystring)
        if response.status_code != 200:
             raise Exception("HTTP Error %d: from %s" % (response.status_code, url))
        return json.loads(response.text.encode('utf8'))["token"]

    @task(task_id='workspace')
    def create_workspace(org, team, ace, name, token):
        '''Create a workspace in a given org for the authenticated user'''
        url = f'https://api.ngc.nvidia.com/v2/org/{org}/workspaces/'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
         }
        data = {
          'aceName': f'{ace}',
          'name': f'{name}'
         }
        response = requests.request("POST", url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            raise Exception("HTTP Error %d: from '%s'" % (response.status_code, url))
        return response.json()


    workspace = create_workspace(org='iffx7vlsrd5t', team='nvbc-tme', ace='nv-launchpad-bc-iad-ace', name='Sydney Kropp', token=get_token(org='iffx7vlsrd5t', team='nvbc-tme'))
    api_connect = find_api_key()
    token = get_token(org='iffx7vlsrd5t', team='nvbc-tme')
    
    # Dummy functions
    start= EmptyOperator(task_id='start')
    end= EmptyOperator(task_id='end')

    # Create a simple workflow
start >> api_connect >> token >> workspace >> end