
import requests
import json
import time
from urllib.parse import urljoin
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class InformaticaCloudHook(BaseHook):
    """
    Hook class for interacting with Informatica Cloud.

    :param informatica_cloud_conn_id: The Airflow connection ID for Informatica Cloud.
    :type informatica_cloud_conn_id: str
    """

    def __init__(self, informatica_cloud_conn_id):
        self.conn = self.get_connection(informatica_cloud_conn_id)
        self.base_url = self.conn.host
        self.session = requests.Session()
        self.session_id = None
        self.server_url = None

    def get_conn(self):
        """
        Returns the Informatica Cloud connection object.
        """
        return self.conn

    def get_session_id(self):
        """
        Retrieves the session ID by making a REST API call to the login endpoint.
        """
        if not self.session_id:
            login_url = urljoin(self.base_url, '/ma/api/v2/user/login')
            headers = {
                    "Content-Type" : "application/json",
                     "Accept" : "application/json"
            }
            response = self.session.post(login_url,headers=headers,data=json.dumps({"@type" : "login", 
                                                    "username": self.conn.login, 
                                                    "password": self.conn.password}))
            response.raise_for_status()
            self.session_id = response.json()['icSessionId']
            self.server_url = response.json()['serverUrl']
            self.session.headers.update({'X-InfaSession': self.session_id})

    def get_federation_id(self, asset_path, asset_type):
        """
        Retrieves the federation ID of an Informatica Cloud asset.

        :param asset_path: The name of the asset.
        :type asset_path: str
        :param asset_type: The type of the asset (e.g., 'mappingTask', 'taskflow', 'fileIngestionTask').
        :type asset_type: str

        :return: The federation ID of the asset.
        :rtype: str
        """
        self.get_session_id()
        lookup_url = str(self.server_url) + '/public/core/v3/lookup'
        print(lookup_url)
        headers = {
            "Content-Type" : "application/json",
            "Accept" : "application/json",
            "INFA-SESSION-ID" : self.session_id
        }
        body = json.dumps({"objects" :[{"path": asset_path, "type" : asset_type}]})
        response = self.session.post(lookup_url,headers=headers,data=body)
        response.raise_for_status()
        return response.json().get("objects")[0].get("id")
    

class MappingTaskOperator(BaseOperator):
    """
    Operator for triggering and monitoring a mapping task in Informatica Cloud.

    :param mapping_task_name: The name of the mapping task.
    :type mapping_task_name: str
    :param informatica_cloud_conn_id: The Airflow connection ID for Informatica Cloud.
    :type informatica_cloud_conn_id: str
    :param polling_time: Time in seconds to poll for mapping task status
    :type polling_time: int
    """

    @apply_defaults
    def __init__(self, mapping_task_name : str, 
                 informatica_cloud_conn_id : str,
                 polling_time: int = 20,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_task_name = mapping_task_name
        self.informatica_cloud_conn_id = informatica_cloud_conn_id
        self.polling_time = polling_time
        self.hook = None

    def execute(self, context):
        self.hook = InformaticaCloudHook(self.informatica_cloud_conn_id)
        federation_id = self.hook.get_federation_id(self.mapping_task_name,asset_type='MTT')
        headers = {
            "Content-Type" : "application/json",
            "Accept" : "application/json",
            "icSessionId" : self.hook.session_id
        }
        job_trigger_url = str(self.hook.server_url) + '/api/v2/job'
        payload = json.dumps({"@type":"job",
                              "taskFederatedId" : federation_id ,
                              "taskType" : "MTT",
                                "runtime": {
                                    "@type": "mtTaskRuntime"
                            }})
        response = self.hook.session.post(job_trigger_url, headers=headers, data=payload)
        response.raise_for_status()
        task_id = response.json()["taskId"]
        run_id = response.json()['runId']
        task_name = response.json()['taskName']
        self.log.info(f'Triggered mapping task {self.mapping_task_name} with Task ID {task_id} and  run ID {run_id}')
        activity_monitor_url = str(self.hook.server_url) + f"/api/v2/activity/activityMonitor?taskId={task_id}&runId={run_id}"
        while True:
            response = self.hook.session.get(activity_monitor_url,headers=headers)
            response.raise_for_status()
            if len(response.json()) == 0:
                break
            time.sleep(self.polling_time)
        activity_log_url = str(self.hook.server_url) + f"/api/v2/activity/activityLog?taskId={task_id}&runId={run_id}"
        response = self.hook.session.get(activity_log_url,headers=headers)
        status = response.json()[0].get('state')
        if str(status) == '1':
            self.log.info(f'Mapping task {self.mapping_task_name} completed successfully')
        else:
            raise Exception(f'Mapping task {self.mapping_task_name} failed.')


