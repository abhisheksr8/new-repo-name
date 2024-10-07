from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def ShellScript():
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "ShellScript", bash_command = "ls -ltr", )
