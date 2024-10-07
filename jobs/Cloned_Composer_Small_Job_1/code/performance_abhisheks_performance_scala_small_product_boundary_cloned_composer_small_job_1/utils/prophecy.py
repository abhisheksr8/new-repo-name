from airflow.decorators import task

db_pipeline_id_to_path_dict = {
    "pipelines/Cloned_Medium_Pipeline_0": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Medium_Pipeline_0.jar", 
    "pipelines/Cloned_Medium_Pipeline_1": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Medium_Pipeline_1.jar", 
    "pipelines/Cloned_Small_Pipeline_0": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_0.jar", 
    "pipelines/Cloned_Small_Pipeline_1": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_1.jar", 
    "pipelines/Cloned_Small_Pipeline_2": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_2.jar", 
    "pipelines/Cloned_Small_Pipeline_3": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_3.jar", 
    "pipelines/Cloned_Small_Pipeline_4": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_4.jar", 
    "pipelines/Cloned_Small_Pipeline_5": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_5.jar", 
    "pipelines/Cloned_Small_Pipeline_6": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_6.jar", 
    "pipelines/Cloned_Small_Pipeline_7": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_7.jar", 
    "pipelines/Cloned_Small_Pipeline_8": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_8.jar", 
    "pipelines/Cloned_Small_Pipeline_9": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Small_Pipeline_9.jar", 
    "pipelines/Cloned_Very_Small_Pipeline_0": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Very_Small_Pipeline_0.jar", 
    "pipelines/Cloned_Very_Small_Pipeline_1": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Very_Small_Pipeline_1.jar", 
    "pipelines/Cloned_Very_Small_Pipeline_2": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Very_Small_Pipeline_2.jar", 
    "pipelines/Cloned_Very_Small_Pipeline_3": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Very_Small_Pipeline_3.jar", 
    "pipelines/Cloned_Very_Small_Pipeline_4": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Cloned_Very_Small_Pipeline_4.jar", 
    "pipelines/Medium_Pipeline": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Medium_Pipeline.jar", 
    "pipelines/Small_Pipeline": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Small_Pipeline.jar", 
    "pipelines/Very_Small_Pipeline": "dbfs:/FileStore/prophecy/artifacts/performance/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/Very_Small_Pipeline.jar"
}


def task_wrapper(task_id):

    def decorator(func):

        @task(task_id = task_id)
        def wrapper(*args, **context):
            ## running the actual method.
            return func(*args, **context).execute(context)

        return wrapper

    return decorator

pipeline_package_name = {
    "pipelines/Cloned_Small_Pipeline_6": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Medium_Pipeline_0": "io.prophecy.pipelines.medium_pipeline.Main", 
    "pipelines/Cloned_Very_Small_Pipeline_3": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_0": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Small_Pipeline": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Very_Small_Pipeline_0": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_2": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_5": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Medium_Pipeline": "io.prophecy.pipelines.medium_pipeline.Main", 
    "pipelines/Cloned_Very_Small_Pipeline_4": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_9": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Medium_Pipeline_1": "io.prophecy.pipelines.medium_pipeline.Main", 
    "pipelines/Cloned_Very_Small_Pipeline_1": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_4": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_3": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_8": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_7": "io.prophecy.pipelines.small_pipeline.Main", 
    "pipelines/Very_Small_Pipeline": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Very_Small_Pipeline_2": "io.prophecy.pipelines.very_small_pipeline.Main", 
    "pipelines/Cloned_Small_Pipeline_1": "io.prophecy.pipelines.small_pipeline.Main"
}
