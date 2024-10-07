from performance_abhisheks_performance_scala_small_product_boundary_cloned_composer_small_job_0.utils import *

def Slack_1_1_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1_1_1",
        text = "Python Sanity Job Run",
        channel = "abhyslackpub",
        slack_conn_id = "slack_default",
        retries = 0
    )
