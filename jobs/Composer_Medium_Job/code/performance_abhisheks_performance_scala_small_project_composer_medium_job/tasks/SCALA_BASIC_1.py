from performance_abhisheks_performance_scala_small_project_composer_medium_job.utils import *

@task_wrapper(task_id = "SCALA_BASIC_1")
def SCALA_BASIC_1(ti=None, params=None, **context):
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "SCALA_BASIC_1",
        json = {
          "task_key": "SCALA_BASIC_1", 
          "new_cluster": {
            "node_type_id": "i3.xlarge", 
            "spark_version": "13.3.x-scala2.12", 
            "runtime_engine": "STANDARD", 
            "num_workers": 0.0, 
            "data_security_mode": "SINGLE_USER", 
            "custom_tags": {"ResourceClass" : "SingleNode"}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/Composer_Medium_Job", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "33", 
              "spark.prophecy.tasks": "H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA==", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.metadata.user.id": "12", 
              "spark.master": "local[*, 4]", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": "true", 
              "spark.databricks.isv.product": "prophecy", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.databricks.cluster.profile": "singleNode", 
              "spark.prophecy.execution.service.url": "wss://performance.cloud.prophecy.io/execution/eventws"
            }, 
            "aws_attributes": {
              "first_on_demand": 1.0, 
              "availability": "SPOT_WITH_FALLBACK", 
              "zone_id": "auto", 
              "spot_bid_price_percent": 100.0
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}, 
            "enable_elastic_disk": False
          }, 
          "spark_jar_task": {
            "main_class_name": "io.prophecy.pipelines.small_pipeline.Main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{
                           "maven": {
                             "coordinates": "io.prophecy:prophecy-libs_2.12:3.4.0-8.3.0-SNAPSHOT", 
                             "repo": "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                           }
                         },                          {
                           "jar": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Small_Pipeline.jar"
                         },                          {"maven" : {"coordinates" : "mysql:mysql-connector-java:8.0.29", "exclusions" : []}}]
        },
        databricks_conn_id = "databricks_default",
    )
