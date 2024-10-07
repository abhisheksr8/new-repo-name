import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from performance_abhisheks_performance_scala_small_product_boundary_cloned_composer_small_job_0.tasks import (
    Python_1_1,
    SM_IO_SCALA_BASIC_1_1,
    SM_IO_SCALA_BASIC_1_2,
    TaskGroup_1_1_tg
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "performance_abhisheks_performance_Scala_Small_Product_Boundary_Cloned_Composer_Small_Job_0", 
    schedule_interval = "0 12 * * 1,4-5", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    params = {
      'c_pipeline_name_scala_basic': Param(
        """pipelines/SCALA_BASIC""", 
        type = "string", 
        title = """c_pipeline_name_scala_basic"""
      )
    }, 
    start_date = pendulum.today('UTC'), 
    end_date = pendulum.datetime(2025, 8, 3, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    SM_IO_SCALA_BASIC_1_1_op = SM_IO_SCALA_BASIC_1_1()
    Python_1_1_op = Python_1_1()
    TaskGroup_1_1_op = TaskGroup_1_1_tg()
    SM_IO_SCALA_BASIC_1_2_op = SM_IO_SCALA_BASIC_1_2()
    Python_1_1_op >> TaskGroup_1_1_op
    SM_IO_SCALA_BASIC_1_1_op >> Python_1_1_op
    TaskGroup_1_1_op >> SM_IO_SCALA_BASIC_1_2_op
