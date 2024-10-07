from airflow.decorators import task_group
from .ForEachLoop_1_tasks import *
from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

@task_group(group_id = "ForEachLoop_1", default_args = {})
def ForEachLoop_1_tg(value):

    @task(task_id = "Python_2")
    def Python_2_op(value, **context):
        return Python_2(value["c1_data"][0]["data"], value["c2_data"][0]["data"], value["c3_data"], value["c3_data"], **context)\
            .execute(context)

    @task(task_id = "xcom_push_test")
    def xcom_push_test_op(value, **context):
        return xcom_push_test(value["c1_data"][0]["data"], value["c2_data"][0]["data"], value["c3_data"], value["c3_data"], **context)\
            .execute(context)

    Python_2_call = Python_2_op(value)
    xcom_push_test_call = xcom_push_test_op(value)
