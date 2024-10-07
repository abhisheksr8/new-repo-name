package io.prophecy.pipelines.small_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.small_pipeline.config._
import io.prophecy.pipelines.small_pipeline.config.ConfigStore.interimOutput
import io.prophecy.pipelines.small_pipeline.udfs.UDFs._
import io.prophecy.pipelines.small_pipeline.udfs.Rules._
import io.prophecy.pipelines.small_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.small_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.small_pipeline.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    if (context.config.c_int > -100) {
      val df_src_avro = src_avro(context).interim(
        "graph",
        "2CsHylb0Si7Iu1BwtU25H$$3c8Yrcik6DteZZzAs7Ogp",
        "40EaQ49eizOI6Q0sgDpV2$$tNarnJReknpOLuhqQoArm"
      )
      Lookup_1(context, df_src_avro)
    }
    val df_src_csv_special_char = src_csv_special_char(context).interim(
      "graph",
      "o8K-lNobc6Z8Asi3dRegs$$Buw8lxPhFtSUcFZhGxXbx",
      "Yui765QKx0wOKHaOnjtzk$$Q04y-nxgyv0ZfORhocEUn"
    )
    val df_schema_transform =
      schema_transform(context, df_src_csv_special_char).interim(
        "graph",
        "5IEpMUJQMpUIx6Hv3eZVS$$9f4baBrU_1q1LbFl9fY2n",
        "vx64sYjC4vVrmBOSkCOXa$$qpmxU6WcJBG1bBJ5VrLY-"
      )
    val df_reformatted_columns_1 = reformatted_columns_1.apply(
      reformatted_columns_1.config
        .Context(context.spark, context.config.reformatted_columns_1),
      df_src_csv_special_char
    )
    val df_reformat_columns =
      reformat_columns(context, df_reformatted_columns_1).interim(
        "graph",
        "9SxukrkbLjB9767nnjjyc$$HKrQPtYnAcfBjk1YLp12B",
        "q2OHFdKSlaApfiB-p2kod$$gjcVI8oVJu6r8jvarb7gm"
      )
    val df_Reformat_11 = Reformat_11(context, df_reformat_columns).interim(
      "graph",
      "Xd3Wt7qKtiH4SPH5A-eGR$$oZwwQphp8b9ewzTGxxg9_",
      "BrNFqH0kpdDq56WRlmzeU$$4SWO5koqKXjR7QpYwUKZ5"
    )
    df_Reformat_11.cache().count()
    df_Reformat_11.unpersist()
    val df_apply_function =
      if (context.config.c_int > -100)
        if (context.config.c_int > -100)
          apply_function(context, df_src_csv_special_char)
            .cache()
            .interim("graph",
                     "yyqmKt6lIbvCdCmjxPmht$$g2IqhVcYuJWg9juCxyjBj",
                     "KOUvK4u2zMac-9BLVt8Is$$CspK8C6VwzsmBNgylSibS"
            )
        else df_src_csv_special_char
      else null
    val df_reformatted_columns = if (context.config.c_int > -100) {
      val df_join_by_c_short =
        join_by_c_short(context, df_apply_function, df_schema_transform)
          .cache()
          .interim("graph",
                   "zUezgrxzF588txPNf9wMI$$JVWCUQIsrKou8fBiESaqS",
                   "hF6jZ7HJrWnao2RvRyYbB$$xQSJSKZgOIG6LRBb7CP6M"
          )
      reformatted_columns(context, df_join_by_c_short).interim(
        "graph",
        "Jsldsl3d5xD4SRjpdKI-Z$$DuCm45gqMbljkiKqBMQjw",
        "sPVo9omm0xwOufqyXCGI8$$F12AVA_FrP1_w296nytQb"
      )
    } else
      null
    val df_DONOT_DELETE =
      DONOT_DELETE(context, df_src_csv_special_char).interim(
        "graph",
        "QVkuAsxoq5NQ0MUxMEpMp$$-059MNIlT6AakSkqptOSR",
        "GDf7WQpZhqZc_HPKUwIc1$$t_F8ZP7gTEFITFhhzi1N1"
      )
    df_DONOT_DELETE.cache().count()
    df_DONOT_DELETE.unpersist()
    val df_UNIT_TESTS_REFORMAT_GEM =
      UNIT_TESTS_REFORMAT_GEM(context, df_src_csv_special_char).interim(
        "graph",
        "hxT8l69zffXuREzW1_qxS$$WeWd0H45ReziLQFnxuqiw",
        "rmK2EW187QKpmEf_ej8gv$$VvPoBSnSLaYQ3NChU5CkS"
      )
    df_UNIT_TESTS_REFORMAT_GEM.cache().count()
    df_UNIT_TESTS_REFORMAT_GEM.unpersist()
    val df_subgraph_reformat_5 = subgraph_reformat_5.apply(
      subgraph_reformat_5.config
        .Context(context.spark, context.config.subgraph_reformat_5),
      df_src_csv_special_char
    )
    val df_Reformat_7 = Reformat_7(context, df_subgraph_reformat_5).interim(
      "graph",
      "wuThqUc2qpf_FmJCMTj2e$$vhP6lu8CS3r_W42eJUyZ1",
      "H6Z_aAnftBpk4a-USAecW$$DfJIjJOqY8XnYd8OGH-ZP"
    )
    val df_Subgraph_4 = Subgraph_4
      .apply(
        Subgraph_4.config.Context(context.spark, context.config.Subgraph_4),
        df_Reformat_7
      )
      .cache()
    val df_Reformat_10 = Reformat_10(context, df_Subgraph_4).interim(
      "graph",
      "cxC3W007DVzCvlwaUSQWg$$ckcxE7qTOTY5b8BHciWQ2",
      "VJzZuiwZgT0rxBlvzkHAY$$80GGHEDnLkXPhO1zGDxko"
    )
    df_Reformat_10.cache().count()
    df_Reformat_10.unpersist()
    if (context.config.c_int > -100 && context.config.c_int > -100) {
      withSubgraphName("graph", context.spark) {
        withTargetId("dest_test", context.spark) {
          dest_test(context, df_reformatted_columns)
        }
      }
    }
    val df_Reformat_12 = Reformat_12(context, df_schema_transform).interim(
      "graph",
      "SZN5-v9MafvGLy6gCWHUV$$Lg9sLnz9osw0CYf5oJ2s5",
      "k62LBfcGvM6fRriAMxLyy$$Xrcm4rTYYUoUoqGaRW28Z"
    )
    df_Reformat_12.cache().count()
    df_Reformat_12.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/Cloned_Small_Pipeline_4"
    )
    spark.conf.set("spark_config1", "spark_config_value_1")
    spark.conf.set("spark_config2", "spark_config_value_2")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config_value2")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/Cloned_Small_Pipeline_4") {
      spark.withSparkOptimisationsDisabled(graph(context))
    }
  }

}
