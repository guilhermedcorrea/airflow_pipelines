from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks_repos import (
    DatabricksReposCreateOperator,
    DatabricksReposDeleteOperator,
    DatabricksReposUpdateOperator,
)

default_args = {
    "owner": "airflow",
    "databricks_conn_id": "databricks",
}

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_repos_operator"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
    default_args=default_args,
    tags=["example"],
    catchup=False,
) as dag:
    
    repo_path = "/Repos/user@domain.com/demo-repo"
    git_url = "https://github.com/test/test"
    create_repo = DatabricksReposCreateOperator(task_id="create_repo", repo_path=repo_path, git_url=git_url)
   
    repo_path = "/Repos/user@domain.com/demo-repo"
    update_repo = DatabricksReposUpdateOperator(task_id="update_repo", repo_path=repo_path, branch="releases")
    

    notebook_task_params = {
        "new_cluster": {
            "spark_version": "9.1.x-scala2.12",
            "node_type_id": "r3.xlarge",
            "aws_attributes": {"availability": "ON_DEMAND"},
            "num_workers": 8,
        },
        "notebook_task": {
            "notebook_path": f"{repo_path}/PrepareData",
        },
    }

    notebook_task = DatabricksSubmitRunOperator(task_id="notebook_task", json=notebook_task_params)


    repo_path = "/Repos/user@domain.com/demo-repo"
    delete_repo = DatabricksReposDeleteOperator(task_id="delete_repo", repo_path=repo_path)


    (create_repo >> update_repo >> notebook_task >> delete_repo)

    from tests.system.utils.watcher import watcher

   
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run 

test_run = get_test_run(dag)