from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import time
import json
import base64


server_url = "https://poc.datagaps.com"
client_id = "dataopssuite-restapi-client"
client_secret = "ouoC2DsI"
username = "pravalika.kumbham"
password = "U2FsdGVkX19g32ZMdE/R4jUMxuDr9ZQw4csnXy14xTA="


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
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


def trigger_dataflow(bearer_token, dataflow_id):

    url = f"{server_url}/DataFlowService/api/v1.0/dataFlows/executeDataFlow?dataflowId={dataflow_id}"

    headers = {
        "Authorization": bearer_token,
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)
    response.raise_for_status()

    run_id = response.json().get("dataFlowRunId")

    print(f"Triggered DataFlow. Run ID: {run_id}")

    return run_id



def check_dataflow_status(run_id, bearer_token):

    url = f"{server_url}/DataFlowService/api/v1.0/dataFlows/dataflow-status?dataFlowRunId={run_id}"

    headers = {"Authorization": bearer_token}

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        status = response.json().get("status", "").upper()

        print(f"DataFlow Status: {status}")

        if status in ["COMPLETED", "FAILED", "ERROR", "SUCCESS"]:
            return status

        time.sleep(90)


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


def run_dataflow_and_pipeline():

    bearer_token = authenticate()

    dataflow_id = "fa0cddb5-e168-4b7a-b370-2ada8d4243c7"
    df_run_id = trigger_dataflow(bearer_token, dataflow_id)

    df_status = check_dataflow_status(df_run_id, bearer_token)

    print(f"Final DataFlow Status: {df_status}")

    if df_status in ["COMPLETED", "SUCCESS"]:

        pipeline_id = "54d6c967-c270-493f-8275-6f90679ac899"

        pl_run_id = trigger_pipeline(bearer_token, pipeline_id)

        pl_status = check_pipeline_status(pl_run_id, bearer_token)

        print(f"Final Pipeline Status: {pl_status}")

    else:
        print("Skipping pipeline execution since DataFlow failed.")



with DAG(
    dag_id="Airflow_Demo_DG_Trigger",
    default_args=default_args,
    max_active_runs=1,
    schedule=None,
    catchup=False,
    tags=["dataflow", "pipeline"],
) as dag:

    trigger_jobs = PythonOperator(
        task_id="trigger_dataflow_and_pipeline",
        python_callable=run_dataflow_and_pipeline,
    )
