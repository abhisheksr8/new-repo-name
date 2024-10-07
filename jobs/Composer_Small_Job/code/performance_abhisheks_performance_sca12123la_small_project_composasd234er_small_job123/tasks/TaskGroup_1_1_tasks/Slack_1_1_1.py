from performance_abhisheks_performance_sca12123la_small_project_composasd234er_small_job123.utils import *

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
