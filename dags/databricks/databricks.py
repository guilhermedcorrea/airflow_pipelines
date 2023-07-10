from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_operator"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 5, 1),
    tags=["example"],
    catchup=False,
) as dag:
   
    new_cluster = {
        "spark_version": "9.1.x-scala2.12",
        "node_type_id": "r3.xlarge",
        "aws_attributes": {"availability": "ON_DEMAND"},
        "num_workers": 8,
    }

    notebook_task_params = {
        "new_cluster": new_cluster,
        "notebook_task": {
            "notebook_path": "/Users/airflow@example.com/PrepareData",
        },
    }

    notebook_task = DatabricksSubmitRunOperator(task_id="notebook_task", json=notebook_task_params)

    spark_jar_task = DatabricksSubmitRunOperator(
        task_id="spark_jar_task",
        new_cluster=new_cluster,
        spark_jar_task={"main_class_name": "com.example.ProcessData"},
        libraries=[{"jar": "dbfs:/lib/etl-0.1.jar"}],
    )

    notebook_task >> spark_jar_task

    from tests.system.utils.watcher import watcher


    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run 


test_run = get_test_run(dag)