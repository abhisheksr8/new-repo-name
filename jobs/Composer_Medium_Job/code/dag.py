import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from performance_abhisheks_performance_scala_small_project_composer_medium_job.tasks import (
    Branch_1_1,
    Email_1,
    Email_1_1,
    Email_2,
    ForEachLoop_1_tg,
    Get_file_format,
    HTTPSensor_1,
    Python_1,
    S3FileSensor_1,
    SCALA_BASIC,
    SCALA_BASIC_1,
    SCALA_BASIC_1_1,
    SM_IO_SCALA_BASIC,
    SM_IO_SCALA_BASIC_1,
    Script_1,
    ShellScript,
    Slack_1,
    TaskGroup_1_tg,
    TriggerDag_1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "performance_abhisheks_performance_Scala_Small_Project_Composer_Medium_Job", 
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
    SM_IO_SCALA_BASIC_op = SM_IO_SCALA_BASIC()
    ShellScript_op = ShellScript()
    SM_IO_SCALA_BASIC_1_op = SM_IO_SCALA_BASIC_1()
    TriggerDag_1_op = TriggerDag_1()
    SCALA_BASIC_op = SCALA_BASIC()
    Email_1_op = Email_1()
    Slack_1_op = Slack_1()
    Branch_1_1_op = Branch_1_1()
    Email_2_op = Email_2()
    Get_file_format_op = Get_file_format()
    Email_1_1_op = Email_1_1()
    Python_1_op = Python_1()
    ForEachLoop_1_op = ForEachLoop_1_tg.expand(value = Python_1_op.output)
    HTTPSensor_1_op = HTTPSensor_1()
    Script_1_op = Script_1()
    SCALA_BASIC_1_op = SCALA_BASIC_1()
    S3FileSensor_1_op = S3FileSensor_1()
    SCALA_BASIC_1_1_op = SCALA_BASIC_1_1()
    TaskGroup_1_op = TaskGroup_1_tg()
    HTTPSensor_1_op >> SCALA_BASIC_1_op
    SM_IO_SCALA_BASIC_1_op >> Python_1_op
    SM_IO_SCALA_BASIC_op >> Script_1_op
    TriggerDag_1_op >> SCALA_BASIC_op
    ShellScript_op >> [Branch_1_1_op, TriggerDag_1_op]
    Branch_1_1_op >> [Email_1_1_op, Email_2_op]
    Script_1_op >> S3FileSensor_1_op
    SCALA_BASIC_op >> [Email_1_op, HTTPSensor_1_op]
    S3FileSensor_1_op >> SCALA_BASIC_1_1_op
    Python_1_op >> [ForEachLoop_1_op, TaskGroup_1_op]
    Email_1_op >> Slack_1_op
