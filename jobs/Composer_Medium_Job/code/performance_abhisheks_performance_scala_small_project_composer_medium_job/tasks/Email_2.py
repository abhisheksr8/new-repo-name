from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

def Email_2():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_2",
        to = "abhisheks@prophecy.io",
        subject = "Hello sir real one",
        html_content = "This one is for python sanity email. Correct one buddy",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "email_default",
    )
