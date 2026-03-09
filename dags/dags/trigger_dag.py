import requests
import time
import uuid
from datetime import datetime, timezone


ASTRO_BASE_URL = "https://cmmisad1h410501nxyoj82nr3.astronomer.run/d4qkgxwm/api/v2"
API_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImNsb2Q0aGtqejAya3AwMWozdWNqbzJwOHIiLCJ0eXAiOiJKV1QifQ.eyJhcGlUb2tlbklkIjoiY21taXpxcnVhNDBqeDAxcDhuN2VvaWx3eCIsImF1ZCI6ImFzdHJvbm9tZXItZWUiLCJpYXQiOjE3NzMwNDkyNDgsImlzQXN0cm9ub21lckdlbmVyYXRlZCI6dHJ1ZSwiaXNJbnRlcm5hbCI6ZmFsc2UsImlzcyI6Imh0dHBzOi8vYXBpLmFzdHJvbm9tZXIuaW8iLCJraWQiOiJjbG9kNGhranowMmtwMDFqM3Vjam8ycDhyIiwicGVybWlzc2lvbnMiOlsiYXBpVG9rZW5JZDpjbW1penFydWE0MGp4MDFwOG43ZW9pbHd4Iiwib3JnYW5pemF0aW9uSWQ6Y21taXNhZDFoNDEwNTAxbnh5b2o4Mm5yMyIsIm9yZ1Nob3J0TmFtZTpjbW1pc2FkMWg0MTA1MDFueHlvajgybnIzIl0sInNjb3BlIjoiYXBpVG9rZW5JZDpjbW1penFydWE0MGp4MDFwOG43ZW9pbHd4IG9yZ2FuaXphdGlvbklkOmNtbWlzYWQxaDQxMDUwMW54eW9qODJucjMgb3JnU2hvcnROYW1lOmNtbWlzYWQxaDQxMDUwMW54eW9qODJucjMiLCJzdWIiOiJjbW1pczd4bDUzeHg4MDFwcTVzYzMya3o5IiwidmVyc2lvbiI6ImNtbWl6cXJ1YTQwancwMXA4M3M1MXRkNW8ifQ.NM5zEArgOXLbVyx8DPuvViLmX3jreKr4s3acHkqCR9IICDuRG3tTfR8RSCxNzNa_21Nr6qmzEGHP5LbdlPjk7mBRUgSiqL9g7e99esHy_5rfcwcJVVMDEQdfJxpAeaKClP-NO_9_zPS39HHzdrzeknsD0qQF4RjW4xGDl_lpENhYaGG8itWLk4KjaSrXfE9KrxhrJ8cEcJywaXOISVf2Xl-o6XycT98ZdDJBzLRotgEFKMd7Rvhji7eKrB9rSsVElk512ICwJrP8ytIZkHVyHumQQ9radxlKzQHbyHU9EbH4MyQAudRwztJFdwjS8J6PYL3a4AuC4jFbcsPrEMO1YQ"

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


dag_id = "datagaps_trigger_dag"
dag_run_id = trigger_dag(dag_id)


if dag_run_id:
    final_status = get_dag_final_status(dag_id, dag_run_id)
