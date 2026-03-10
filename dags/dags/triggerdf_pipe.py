from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import time
import base64

server_url = "https://poc.datagaps.com"
client_id = "dataopssuite-restapi-client"
client_secret = "ouoC2DsI"
username = "pravalika.kumbham"
password = "U2FsdGVkX19g32ZMdE/R4jUMxuDr9ZQw4csnXy14xTA="

default_args = {
    "retries": 0,
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 10),
}


def authenticate():
    auth_url = f"{server_url}/dataopssecurity/oauth2/token"

    basic_auth_str = f"{client_id}:{client_secret}"
    base64_auth_str = base64.b64encode(basic_auth_str.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {base64_auth_str}"
    }

    payload = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }

    response = requests.post(auth_url, headers=headers, data=payload)
    response.raise_for_status()

    token = response.json().get("access_token")

    if not token:
        raise Exception("Authentication failed: No access token received")

    return f"Bearer {token}"


def trigger_pipeline(bearer_token, pipeline_id):

    url = f"{server_url}/piper/jobs"

    headers = {
        "Authorization": bearer_token,
        "Content-Type": "application/json"
    }

    payload = {
        "pipelineId": pipeline_id,
        "runName": "Triggered from Airflow"
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()

    run_id = data.get("id") or data.get("jobId")

    print(f"Triggered Pipeline. Run ID: {run_id}")

    return run_id


def check_pipeline_status(run_id, bearer_token):

    url = f"{server_url}/piper/jobs/{run_id}/status"

    headers = {"Authorization": bearer_token}

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        status = response.json().get("status")

        print(f"Pipeline Status: {status}")

        if status in ["COMPLETED", "FAILED", "ERROR"]:
            print(f"Pipeline finished with status: {status}")
            return status

        time.sleep(300)


def run_pipeline():

    bearer_token = authenticate()

    pipeline_id = "54d6c967-c270-493f-8275-6f90679ac899"

    pl_run_id = trigger_pipeline(bearer_token, pipeline_id)

    pl_status = check_pipeline_status(pl_run_id, bearer_token)

    print(f"Final Pipeline Status: {pl_status}")


with DAG(
    dag_id="Airflow_Demo_DG_Trigger",
    default_args=default_args,
    max_active_runs=1,
    schedule=None,
    catchup=False,
    tags=["pipeline"],
) as dag:

    trigger_pipeline_task = PythonOperator(
        task_id="trigger_pipeline",
        python_callable=run_pipeline,
    )
