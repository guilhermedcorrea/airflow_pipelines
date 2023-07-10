from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksCopyIntoOperator,
    DatabricksSqlOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_sql_operator"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
    tags=["example"],
    catchup=False,
) as dag:
    connection_id = "my_connection"
    sql_endpoint_name = "My Endpoint"

 
    create = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="create_and_populate_table",
        sql=[
            "drop table if exists default.my_airflow_table",
            "create table default.my_airflow_table(id int, v string)",
            "insert into default.my_airflow_table values (1, 'test 1'), (2, 'test 2')",
        ],
    )

    select = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="select_data",
        sql="select * from default.my_airflow_table",
    )
   
    select_into_file = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="select_data_into_file",
        sql="select * from default.my_airflow_table",
        output_path="/tmp/1.jsonl",
        output_format="jsonl",
    )

    create_file = DatabricksSqlOperator(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        task_id="create_and_populate_from_file",
        sql="test.sql",
    )
 
    import_csv = DatabricksCopyIntoOperator(
        task_id="import_csv",
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        table_name="my_table",
        file_format="CSV",
        file_location="abfss://container@account.dfs.core.windows.net/my-data/csv",
        format_options={"header": "true"},
        force_copy=True,
    )
  
    (create >> create_file >> import_csv >> select >> select_into_file)

    from tests.system.utils.watcher import watcher


    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run 


test_run = get_test_run(dag)