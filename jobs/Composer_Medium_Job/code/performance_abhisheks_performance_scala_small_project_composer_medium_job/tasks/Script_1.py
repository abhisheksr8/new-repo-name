from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def Script_1():
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_1", bash_command = "echo \"test\"", )
