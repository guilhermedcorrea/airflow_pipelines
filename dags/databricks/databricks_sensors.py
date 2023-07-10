from __future__ import annotations

import os
import textwrap
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor

# [Env variable to be used from the OS]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
# [DAG name to be shown on Airflow UI]
DAG_ID = "example_databricks_sensor"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    dag.doc_md = textwrap.dedent(
    
    )
 
    connection_id = "databricks_default"
    sql_warehouse_name = "Starter Warehouse"


    sql_sensor = DatabricksSqlSensor(
        databricks_conn_id=connection_id,
        sql_warehouse_name=sql_warehouse_name,
        catalog="hive_metastore",
        task_id="sql_sensor_task",
        sql="select * from hive_metastore.temp.sample_table_3 limit 1",
        timeout=60 * 2,
    )
   
    partition_sensor = DatabricksPartitionSensor(
        databricks_conn_id=connection_id,
        sql_warehouse_name=sql_warehouse_name,
        catalog="hive_metastore",
        task_id="partition_sensor_task",
        table_name="sample_table_2",
        schema="temp",
        partitions={"date": "2023-01-03", "name": ["abc", "def"]},
        partition_operator="=",
        timeout=60 * 2,
    )

    (sql_sensor >> partition_sensor)

    from tests.system.utils.watcher import watcher



    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run 

test_run = get_test_run(dag)