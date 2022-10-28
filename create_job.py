"""Example: Creating a Job using internal API calls.

API paths:
* https://github.com/orchest/orchest/blob/master/services/orchest-webserver/app/app/views/views.py
* https://github.com/orchest/orchest/blob/master/services/orchest-webserver/app/app/views/orchest_api.py
"""
import sys
from contextlib import contextmanager

import requests

# TODO: Insert credentials.
# Would be cleaner to use environment variables with something like:
# https://github.com/theskumar/python-dotenv
INSTANCE_URL = "http://localorchest.io"
INSTANCE_USERNAME = "example"
INSTANCE_PASSWORD = "example"


@contextmanager
def authenticated_session(*args, **kwargs):
    """Gets an authenticated session by persisting the login cookie."""
    session = requests.Session(*args, **kwargs)
    data = {
        "username": INSTANCE_USERNAME,
        "password": INSTANCE_PASSWORD,
    }
    resp = session.post(
        f"{INSTANCE_URL}/login", timeout=4, data=data, allow_redirects=True
    )
    if resp.status_code != 200:
        raise RuntimeError(
            "Failed to create authenticated session: Instance login failed."
        )

    try:
        yield session
    finally:
        session.close()


def create_job(
    session: requests.Session,
    project_uuid: str,
    pipeline_uuid: str
) -> None:
    # Create the Job draft.
    url = f"{INSTANCE_URL}/catch/api-proxy/api/jobs"
    post_data = {
        "project_uuid": project_uuid,
        "pipeline_uuid": pipeline_uuid,
        "pipeline_name": "california-housing",
        "name": "example-job",
        "draft": True,
        "pipeline_run_spec": {"run_type": "full", "uuids": []},
        "parameters": [{}],
        "max_retained_pipeline_runs": 50,
    }
    resp = session.post(url, json=post_data)
    if resp.status_code != 201:
        raise RuntimeError("Failed to create Job draft.")

    job_uuid = resp.json()["uuid"]

    # Start the Job.
    url = f"{INSTANCE_URL}/catch/api-proxy/api/jobs/{job_uuid}"
    post_data = {
        "confirm_draft": True,
        "strategy_json": {},
        "parameters": [{}],
        "cron_schedule": "0 * * * *"
    }
    resp = session.put(url, json=post_data)
    if resp.status_code != 200:
        raise RuntimeError("Failed to start Job.")


def main():
    # You can get `project_uuid` and `pipeline_uuid` from the URL when
    # opening the respective Pipeline in the Pipeline editor.
    # Alternatively, you can query the API to get all projects and
    # pipelines to determine their UUIDs based on names. URLs:
    # f"{INSTANCE_URL}/async/projects"
    # f"{INSTANCE_URL}/async/pipelines/{project_uuid}"
    project_uuid = "84f49b08-11d4-4a13-9c22-11dca7e72e80"
    pipeline_uuid = "0915b350-b929-4cbd-b0d4-763cac0bb69f"

    with authenticated_session() as session:
        try:
            create_job(
                session=session,
                project_uuid=project_uuid,
                pipeline_uuid=pipeline_uuid
            )
        except RuntimeError:
            print("Failed to create a new Job in Orchest.")
            sys.exit(1)

        print("Successfully creating a new Job in Orchest.")


if __name__ == "__main__":
    main()
