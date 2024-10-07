from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def Email_1_1():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1_1",
        to = "abhisheks@prophecy.io",
        subject = "Hello abhisheks",
        html_content = "Python sanity job run buddy",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "email_default",
    )
