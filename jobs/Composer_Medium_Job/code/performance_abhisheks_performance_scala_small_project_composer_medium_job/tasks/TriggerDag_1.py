from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

@task_wrapper(task_id = "TriggerDag_1")
def TriggerDag_1(ti=None, params=None, **context):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from datetime import timedelta

    return TriggerDagRunOperator(
        task_id = "TriggerDag_1",
        trigger_dag_id = "Staging_Shared_Airflow_Composer_Python_dag",
        conf = None,
        reset_dag_run = False, 
        wait_for_completion = False
    )
