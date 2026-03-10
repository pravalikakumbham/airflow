
import requests
import time
import uuid
from datetime import datetime, timezone

ASTRO_BASE_URL = "https://cmmisad1h410501nxyoj82nr3.astronomer.run/dhy24q9q/api/v2"
API_TOKEN ="eyJhbGciOiJSUzI1NiIsImtpZCI6ImNsb2Q0aGtqejAya3AwMWozdWNqbzJwOHIiLCJ0eXAiOiJKV1QifQ.eyJhcGlUb2tlbklkIjoiY21tamZkdW5qNGE3cjAxbnh1Nml5cWhyMSIsImF1ZCI6ImFzdHJvbm9tZXItZWUiLCJpYXQiOjE3NzMwNzU1MTksImlzQXN0cm9ub21lckdlbmVyYXRlZCI6dHJ1ZSwiaXNJbnRlcm5hbCI6ZmFsc2UsImlzcyI6Imh0dHBzOi8vYXBpLmFzdHJvbm9tZXIuaW8iLCJraWQiOiJjbG9kNGhranowMmtwMDFqM3Vjam8ycDhyIiwicGVybWlzc2lvbnMiOlsiYXBpVG9rZW5JZDpjbW1qZmR1bmo0YTdyMDFueHU2aXlxaHIxIiwib3JnYW5pemF0aW9uSWQ6Y21taXNhZDFoNDEwNTAxbnh5b2o4Mm5yMyIsIm9yZ1Nob3J0TmFtZTpjbW1pc2FkMWg0MTA1MDFueHlvajgybnIzIl0sInNjb3BlIjoiYXBpVG9rZW5JZDpjbW1qZmR1bmo0YTdyMDFueHU2aXlxaHIxIG9yZ2FuaXphdGlvbklkOmNtbWlzYWQxaDQxMDUwMW54eW9qODJucjMgb3JnU2hvcnROYW1lOmNtbWlzYWQxaDQxMDUwMW54eW9qODJucjMiLCJzdWIiOiJjbW1pczd4bDUzeHg4MDFwcTVzYzMya3o5IiwidmVyc2lvbiI6ImNtbWpmZHVuajRhN3EwMW54OGFscHd6d3UifQ.KPWSMubyGIcitsjVzD849Srwxg8nZeVlAJNs8vbniKuuc5RSKmnjiJDfLKYdOfWLRr0yEKOOFxhHGM0lS_FNlKCmKCRFXHX6bJVQTdMGfM4hwqunVmeFbP-ahDA0NMhZ5heXzw5CqD6_GF0FGgK9q8NOkf6h7eBRgpnzgPsh1jdUWMTq-eRrpYPVqqB7qz00TbMttqRXi6MryGCxhn58PJmQrvWWxPVxH0lWlISrQ7U8VsnYII_AShbwNyJL5dmaJ3JjdpLcxmCzyFhp3ivG9tYjpYqM6xAIzS7KOW7bLfWrJVv_PKGGAsr_gPg6W0fytiNc6F8WJVQ8pf2jCV5PmQ"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def set_dag_toggle(dag_id, pause: bool):

    url = f"{ASTRO_BASE_URL}/dags/{dag_id}"
    payload = {"is_paused": pause}   
    try:
        response = requests.patch(url, headers=headers, json=payload)
        if response.status_code in [200, 201]:
            print(f"DAG '{dag_id}' is now {'OFF (Paused)' if pause else 'ON (Unpaused)'}")
        else:
            print(f"Failed to toggle DAG. Status: {response.status_code}")
            print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print("Error while toggling DAG:", e)
def trigger_dag(dag_id):
   
    url = f"{ASTRO_BASE_URL}/dags/{dag_id}/dagRuns"
    batch_id = str(uuid.uuid4())
    logical_date = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    payload = {
        "dag_run_id": f"manual__{logical_date}",
        "logical_date": logical_date, 
        "conf": {
            "batch_id": batch_id,
            "trigger_time": logical_date,
        },
    }

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
        print("Error while triggering the DAG:", e)
        return None


def get_dag_final_status(dag_id, dag_run_id):

    url = f"{ASTRO_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"

    max_checks = 20
    check_count = 0

    try:
        while check_count < max_checks:

            response = requests.get(url, headers=headers)

            if response.status_code == 200:

                data = response.json()
                state = data["state"]

                if state in ["success", "failed", "error"]:
                    print(f"DAG Run completed with status: {state}")
                    return data

                else:
                    print(f"DAG still running... state: {state}")
                    time.sleep(50)
                    check_count += 1

            else:
                print(f"Failed to fetch DAG status. Status Code: {response.status_code}")
                return None

        print("Timeout reached. DAG still not finished.")
        return None

    except requests.exceptions.RequestException as e:
        print("Error while fetching DAG status:", e)
        return None
def check_existing_run(dag_id):
    url = f"{ASTRO_BASE_URL}/dags/{dag_id}/dagRuns?limit=1"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        for run in data["dag_runs"]:
            if run["state"] == "running":
                print("DAG already running. Not triggering again.")
                return True

    return False

dag_id ="Airflow_Demo_DG_Trigger"

set_dag_toggle(dag_id, pause=False)

if not check_existing_run(dag_id):

    dag_run_id = trigger_dag(dag_id)

    set_dag_toggle(dag_id, pause=True)

    if dag_run_id:
        final_status = get_dag_final_status(dag_id, dag_run_id)

else:
    print("Skipping trigger because DAG already running or queued.")
