from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def xcom_push_test(config_1, config_2, config_3, config_4, ti=None, params=None, **context):

    def delta():

        if config_3 == 0:
            ti.xcom_push(key = 'i am from if', value = 1123)
            ti.xcom_push(key = '2nd one from if', value = "check me")
            ti.xcom_push(key = 'passed if', value = "out")
            ti.xcom_push(key = 'lets see', value = 100)
            ti.xcom_push(key = 'i am random', value = 2000)
        else:
            ti.xcom_push(key = 'ELSE here', value = 1123)
            ti.xcom_push(key = 'right here ELSE', value = "check me")
            ti.xcom_push(key = 'find me', value = "out")
            ti.xcom_push(key = 'catchup', value = 100)
            ti.xcom_push(key = 'i am random as well', value = 2000)

        return {"Omega" : "beta", "y" : "x"}

    import json
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(task_id = "xcom_push_test", python_callable = delta, show_return_value_in_logs = True)
