from airflow import DAG # DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

#ให้ DAG ทำงานเมื่อ วันที่ 1 เดือน 2
with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2025,2,1),
    schedule = None,
    tags=['dpu', 'my_firstdag', 'test']
):
    # Create Node by using EmptyOperator
    t1 = EmptyOperator(task_id="t1") 
    t2 = EmptyOperator(task_id="t2") 

    t1 >> t2