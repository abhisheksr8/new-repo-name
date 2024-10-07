package io.prophecy.pipelines.very_small_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.very_small_pipeline.config._
import io.prophecy.pipelines.very_small_pipeline.config.ConfigStore.interimOutput
import io.prophecy.pipelines.very_small_pipeline.udfs.UDFs._
import io.prophecy.pipelines.very_small_pipeline.udfs.Rules._
import io.prophecy.pipelines.very_small_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.very_small_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.very_small_pipeline.graph._
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
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
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
                   "pipelines/Cloned_Very_Small_Pipeline_3"
    )
    spark.conf.set("spark_config1", "spark_config_value_1")
    spark.conf.set("spark_config2", "spark_config_value_2")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config_value2")
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/Cloned_Very_Small_Pipeline_3"
    ) {
      spark.withSparkOptimisationsDisabled(graph(context))
    }
  }

}
