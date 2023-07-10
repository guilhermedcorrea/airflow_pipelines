from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="Trigger_insert_preditivo_tim",
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),
    catchup=False,
    schedule="@once",
    tags=["FAZ INSERT DO RETORNO DA CONSULTA PREDITIVO TIM"],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="insert_retorno_preditivo_tim",
        trigger_dag_id="segmentacao_preditivo_tim",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )
    
    trigger