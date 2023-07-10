from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import json
import pendulum
from airflow.decorators import dag, task





@task(task_id="executa_consultas_preditivo")
def get_segmentacao_preditivo_tim():
    ...

    @task()
    def extract():
        
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)

        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):

        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        print(f"Total order value is: {total_order_value:.2f}")
        order_data = extract()
        order_summary = transform(order_data)
        load(order_summary["total_order_value"])
    
    

with DAG(
    dag_id="segmentacao_preditivo_tim",
    schedule=None,
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),
    catchup=False,
    tags=["ETL PREDITIVO SEGMENTAÇÃO TIM"],
) as dag:
    run_this = get_segmentacao_preditivo_tim()

    bash_task = BashOperator(
        task_id="agrupando_informacoes",
        bash_command='echo "Here is the message: $message"',
        env={"message": '{{ dag_run.conf.get("message") }}'},
    )
    
    bash_task