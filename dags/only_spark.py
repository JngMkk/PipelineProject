#-*- coding: utf-8 -*-
import pendulum
from mod.slackbot import Slack
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

kst = pendulum.timezone('Asia/Seoul')
sb = Slack('#pipeline')

default_args = {
    "owner" : "admin",
    "depends_on_past" : False,
    "wait_for_downstream" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=20),
    "on_failure_callback" : sb.fail,
}

dag = DAG(
    dag_id='SparkOnly',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 3, 29, tzinfo=kst),
    end_date=datetime(2022, 4, 2, tzinfo=kst),
    catchup=False
)

#=================================================
#                      Spark                     #
#=================================================

ciAllSpark = SparkSubmitOperator(
    task_id='ciAllSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_all.py',
    conn_id='spark_default',
    dag=dag
)

ciAcciSpark = SparkSubmitOperator(
    task_id='ciAcciSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_accident.py',
    conn_id='spark_default',
    dag=dag
)

ciInstSpark = SparkSubmitOperator(
    task_id='ciInstSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_inst.py',
    conn_id='spark_default',
    dag=dag
)

ciManageSpark = SparkSubmitOperator(
    task_id='ciManageSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_manager.py',
    conn_id='spark_default',
    dag=dag
)

cahCauseSpark = SparkSubmitOperator(
    task_id='cahCauseSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_cah_cause.py',
    conn_id='spark_default',
    dag=dag
)

ciFloorSpark = SparkSubmitOperator(
    task_id='ciFloorSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_floor_info.py',
    conn_id='spark_default',
    dag=dag
)

ciOperSpark = SparkSubmitOperator(
    task_id='ciOperSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_operation.py',
    conn_id='spark_default',
    dag=dag
)

ciPlaceSpark = SparkSubmitOperator(
    task_id='ciPlaceSpark',
    application='/home/ubuntu/airflow/dags/spark_code/ci_place_info.py',
    conn_id='spark_default',
    dag=dag
)


#=================================================
#                      Slack                     #
#=================================================

slack = PythonOperator(
    task_id='sendmsg',
    python_callable=sb.dbgout,
    op_args=['ALL DONE!'],
    dag=dag
)

[ciAllSpark, ciAcciSpark, ciInstSpark, ciManageSpark, cahCauseSpark, ciFloorSpark, ciOperSpark, ciPlaceSpark]  >> slack
