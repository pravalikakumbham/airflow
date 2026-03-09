import requests
import time
import uuid
from datetime import datetime, timezone


ASTRO_BASE_URL = $[AirflowURL]
API_TOKEN = $[APIToken]

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def trigger_dag(dag_id):
    """Trigger a DAG in Airflow and return the DAG run ID."""
    url = f"{ASTRO_BASE_URL}/dags/{dag_id}/dagRuns"
    batch_id = str(uuid.uuid4())
    logical_date = datetime.now(timezone.utc).isoformat() 
    payload = {"conf": {"batch_id": batch_id}, "logical_date": logical_date}

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code in [200, 201]:
            data = response.json()
            print("DAG Triggered Successfully!")
            return data["dag_run_id"]
        else:
            print(f"Failed to trigger DAG. Status Code: {response.status_code}")
            print("Response:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print("An error occurred while triggering the DAG:", e)
        return None

def get_dag_final_status(dag_id, dag_run_id):
    """Wait until the DAG run is completed, then fetch the final status."""
    url = f"{ASTRO_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"

    try:
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                state = data["state"]

                if state in ["success", "failed", "error"]:
                    print(f"DAG Run completed with status: {state}")
                    print("DAG Run Details:", data)
                    return data
                else:
                    time.sleep(10)
            else:
                print(f"Failed to fetch DAG status. Status Code: {response.status_code}")
                print("Response:", response.text)
                return None
    except requests.exceptions.RequestException as e:
        print("An error occurred while fetching the DAG status:", e)
        return None


dag_id = $[DAGID]
dag_run_id = trigger_dag(dag_id)


if dag_run_id:
    final_status = get_dag_final_status(dag_id, dag_run_id)
