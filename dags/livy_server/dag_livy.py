from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "livy_operator"

with DAG(
    dag_id=DAG_ID,
    default_args={"args": [10]},
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    livy_java_task = LivyOperator(
        task_id="pi_java_task",
        file="/spark-jars.jar",
        num_executors=1,
        conf={
            "spark.shuffle.compress": "false",
        },
        class_name="org.apache.spark.examples.SparkPi",
    )

    livy_python_task = LivyOperator(task_id="pi_python_task", file="/pi.py", polling_interval=60)

    livy_java_task >> livy_python_task
   
    livy_java_task_deferrable = LivyOperator(
        task_id="livy_java_task_deferrable",
        file="/spark-examples.jar",
        num_executors=1,
        conf={
            "spark.shuffle.compress": "false",
        },
        class_name="org.apache.spark.examples.SparkPi",
        deferrable=True,
    )

    livy_python_task_deferrable = LivyOperator(
        task_id="livy_python_task_deferrable", file="/pi.py", polling_interval=60, deferrable=True
    )

    livy_java_task_deferrable >> livy_python_task_deferrable
  

    from tests.system.utils.watcher import watcher


    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run


test_run = get_test_run(dag)