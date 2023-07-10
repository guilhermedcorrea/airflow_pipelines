'''
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


with DAG(dag_id="checa_tempo_produtivo", start_date=days_ago(2), tags=["aguardando_tempo"]) as dag:
    start = DummyOperator(task_id="start")


    with TaskGroup("checa_e_atualiza_tempo_tabela_dim_tempo", tooltip="fazendo_insert_retorno_consulta") as section_1:
        task_1 = DummyOperator(task_id="select_tabela_login_1")
        task_2 = BashOperator(task_id="select_tabela_consulta", bash_command='echo 1')
        task_3 = DummyOperator(task_id="select_tabela_acessos")

        task_1 >> [task_2, task_3]
   

  
    with TaskGroup("checando_e_atualizando_retorno_da_section_1", tooltip="Tasks for section_2") as section_2:
        task_1 = DummyOperator(task_id="task_1")

       
        with TaskGroup("checando_e_atualizando_retorno_da_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="left_join_grupo_1", bash_command='echo 1')
            task_3 = DummyOperator(task_id="select_1")
            task_4 = DummyOperator(task_id="insert_from_retorno_joins")

            [task_2, task_3] >> task_4
        

    end = DummyOperator(task_id='feito')

    start >> section_1 >> section_2 >> end

'''