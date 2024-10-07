from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def HTTPSensor_1():
    from airflow.providers.http.sensors.http import HttpSensor
    from datetime import timedelta

    # Execution timeout is airflow task level execution timeout
    # Sensor timeout will be different. Should be handled separately
    return HttpSensor(
        task_id = "HTTPSensor_1",
        endpoint = "/webhp",
        request_params = None,
        headers = None,
        response_check = lambda response: response.status_code == 200,
        http_conn_id = "http_default",
        poke_interval = 20,
        timeout = 600,
    )
