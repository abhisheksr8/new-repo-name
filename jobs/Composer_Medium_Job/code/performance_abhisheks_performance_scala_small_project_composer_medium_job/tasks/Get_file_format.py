from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def Get_file_format():

    def find_file_path(params, ti, **kwargs):
        print(
            "File format: {}".format(
              get_feed_config(
                load_resource(kwargs['dag_path'], kwargs['dag'].dag_id, kwargs['resource_file_path']),
                ["bm", "hsbc", "camp", "sqoop", "daily"]
              )[5]
            )
        )

    import json
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(
        task_id = "Get_file_format",
        python_callable = find_file_path,
        op_kwargs = {"resource_file_path" : "feed/feed_config.txt", "dag_path" : "/home/airflow/gcs/dags"}, 
        show_return_value_in_logs = True
    )
