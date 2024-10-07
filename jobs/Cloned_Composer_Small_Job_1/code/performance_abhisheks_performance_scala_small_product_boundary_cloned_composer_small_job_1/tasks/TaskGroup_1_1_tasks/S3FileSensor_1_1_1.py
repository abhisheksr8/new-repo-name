from performance_abhisheks_performance_scala_small_product_boundary_cloned_composer_small_job_1.utils import *

def S3FileSensor_1_1_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1_1_1",
        bucket_key = [s.strip() for s in "test/validation_data/test_source.json".split(",") if s.strip()],
        bucket_name = "qa-prophecy",
        check_fn = None,
        aws_conn_id = "aws_default",
        wildcard_match = False,
        verify = False,
        timeout = 600,
    )
