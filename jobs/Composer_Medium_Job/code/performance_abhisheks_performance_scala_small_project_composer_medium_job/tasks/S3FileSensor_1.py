from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def S3FileSensor_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1",
        bucket_key = [s.strip() for s in "dags/*".split(",") if s.strip()],
        bucket_name = "prophecy-mwaa-243",
        check_fn = None,
        aws_conn_id = "aws_default",
        wildcard_match = True,
        verify = True,
        timeout = 600,
    )
