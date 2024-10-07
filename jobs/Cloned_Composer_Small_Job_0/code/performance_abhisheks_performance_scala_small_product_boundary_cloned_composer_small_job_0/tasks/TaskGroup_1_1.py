from airflow.decorators import task_group
from .TaskGroup_1_1_tasks import *
from performance_abhisheks_performance_scala_small_product_boundary_cloned_composer_small_job_0.utils import *

@task_group(group_id = "TaskGroup_1_1", default_args = {})
def TaskGroup_1_1_tg():
    S3FileSensor_1_1_1_op = S3FileSensor_1_1_1()
    HTTPSensor_1_1_1_op = HTTPSensor_1_1_1()
    Slack_1_1_1_op = Slack_1_1_1()
    HTTPSensor_1_1_1_op >> S3FileSensor_1_1_1_op
    S3FileSensor_1_1_1_op >> Slack_1_1_1_op
