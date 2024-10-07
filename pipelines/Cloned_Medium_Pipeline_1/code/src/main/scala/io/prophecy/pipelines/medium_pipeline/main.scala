package io.prophecy.pipelines.medium_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.config._
import io.prophecy.pipelines.medium_pipeline.config.ConfigStore.interimOutput
import io.prophecy.pipelines.medium_pipeline.udfs.UDFs._
import io.prophecy.pipelines.medium_pipeline.udfs.Rules._
import io.prophecy.pipelines.medium_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.medium_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.medium_pipeline.graph._
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
    val df_src_json_input_custs_1 = src_json_input_custs_1(context)
      .cache()
      .interim("graph",
               "mmggGm_ATYkJhpEf9mSQs$$IzOZYTsgHlnHgyvXBHkke",
               "ALcOSH0JysMg5IoYqh1a1$$4i36nv4nR6-7BekBy3P29"
      )
    val df_Reformat_11_1 =
      Reformat_11_1(context, df_src_json_input_custs_1).interim(
        "graph",
        "KePZ1idCCBbZAi689GY3G$$qqrH35sILLuLaU5WLItnq",
        "5PUi-c-stxC-CD2u7JJqC$$Xfsyd_uVCJiIHKimdhUcW"
      )
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
    val df_src_orc_all_type_no_partition =
      src_orc_all_type_no_partition(context).interim(
        "graph",
        "5o8CXPoJdyOd_G7Ka9ghk$$lLEkeV61qwITWhIcTsli2",
        "KsZKW5SjgyjrPA7S01fuA$$9KJhNrwFEtuPIKH-fPNNk"
      )
    val df_falttenSchemaMain =
      falttenSchemaMain(context, df_src_orc_all_type_no_partition)
        .cache()
        .interim("graph",
                 "wn5lPNwhgG2zYPcnhdt7c$$mMX3VV2r8BAN3tIKwqi2N",
                 "44Rd54jl0PaT-puWJxxPl$$w0-giNxZpLnXX_eoMmB9g"
        )
    val df_Deduplicate_2 = Deduplicate_2(context, df_falttenSchemaMain).interim(
      "graph",
      "I_5YRJ4cAYBqJZaWDox9k$$lEn75ogkYwcjUPBkTFRXm",
      "airXZnAMOwFeERGngL3gu$$YlpES18gmr6RVDZX0OSLi"
    )
    val df_order_by_columns =
      order_by_columns(context, df_src_csv_special_char).interim(
        "graph",
        "C54W621yAcHtki5x3DW4t$$mRb4xyty2IwgXY_Knp0Nh",
        "kUnTROe_7Ulj3LawVZAKm$$iahi2CR9UdsqmEV1FeytS"
      )
    val df_reformatted_columns_1 = reformatted_columns_1.apply(
      reformatted_columns_1.config
        .Context(context.spark, context.config.reformatted_columns_1),
      df_order_by_columns
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
    val df_Limit_4 = Limit_4(context, df_Reformat_11).interim(
      "graph",
      "IiA1vesd6ia3Kki3zXD-I$$g6Ys-6ivgCBjvgjTeW2Ze",
      "2Y0tLZqOioaqEIJphm3N5$$b9uhCqED2owJs3Gx-KEa2"
    )
    val df_SubGraph_1 = SubGraph_1.apply(
      SubGraph_1.config.Context(context.spark, context.config.SubGraph_1),
      df_Deduplicate_2
    )
    val df_SchemaTransform_1 =
      SchemaTransform_1(context, df_SubGraph_1).interim(
        "graph",
        "ZXnzuJkizLrvuuuuszi_w$$bzGxAGx0jHykjcjMUEFIm",
        "nOHEALxkYXZlzMlbiGGX-$$_hwkxDtkbQ7lKcKLAlwwl"
      )
    val df_Limit_1 = Limit_1(context, df_SchemaTransform_1).interim(
      "graph",
      "iW7xho2_XgaGLRFgPxB8S$$zoxG4YXHeYz287kPNRvDI",
      "nk_7oP2mTg0j3gyKu0aa2$$jd4Ugry-RFX1-WixSQVUW"
    )
    val df_src_catalog_table_test_catalog_source =
      src_catalog_table_test_catalog_source(context).interim(
        "graph",
        "vmRKV0Nd6-lh2PBG9DCyM$$Ef-xrvLXYFtIl9bXPURGe",
        "RiRMBmFnYhsz_jKLJYl4P$$yjZtPM-ppvSIY53TFNG3v"
      )
    val df_DONOT_DELETE =
      DONOT_DELETE(context, df_src_csv_special_char).interim(
        "graph",
        "QVkuAsxoq5NQ0MUxMEpMp$$-059MNIlT6AakSkqptOSR",
        "GDf7WQpZhqZc_HPKUwIc1$$t_F8ZP7gTEFITFhhzi1N1"
      )
    val df_Limit_5 = Limit_5(context, df_DONOT_DELETE).interim(
      "graph",
      "N5btKhJe3C11sV2V3FLal$$p_iglTRkvOueqCPo-SDEw",
      "6ZVW3ATOcLs1sZI92zOrK$$nBlj2EHWXcjbeQCx7HgPF"
    )
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
    val df_Limit_3 = Limit_3(context, df_Reformat_10).interim(
      "graph",
      "I4PCdGcyTPP_x2NMRMxPo$$LHkYCi7tJiY8j2AMmwD_9",
      "djq-u8yjOh76lutC8wmxE$$uuUSS7LkfUm-X54keusIM"
    )
    val df_Reformat_1 = Reformat_1(context, df_Limit_3).interim(
      "graph",
      "yQdSfBCe2cY20bZuhppnC$$DEf8qbkzn7HaReesM9LYc",
      "gkZSgJ9LbaNc5-eVDxjsw$$1O7iVZgQ0sXR2hdrDA9FI"
    )
    val df_Reformat_2_1 =
      Reformat_2_1(context, df_src_catalog_table_test_catalog_source).interim(
        "graph",
        "6Znk7A4h43eh2eyod9GFr$$tNzoe73BnDjFPgRI4oQ3P",
        "r0a543LnVEGLj4EiHP7P0$$dmEYMEh69BihJ645aPEy7"
      )
    val df_pm_shared_graph =
      if (context.config.double > 0)
        pm_shared_graph.apply(
          pm_shared_graph.config
            .Context(context.spark, context.config.pm_shared_graph),
          df_Reformat_2_1
        )
      else df_Reformat_2_1
    val df_Limit_2 = Limit_2(context, df_pm_shared_graph).interim(
      "graph",
      "RmMzpjnN8qHuc0KM992OB$$rTttFZbOtXbPb3dmhKKff",
      "IR_oyjhWo04kUo4nK_xV9$$HFTdrCiFKRJcc3bZdvmW7"
    )
    val df_CustomReformat_1 = CustomReformat_1(context, df_Limit_2).interim(
      "graph",
      "1_UJt_1fV8dp-2kaFIxO9$$iBiVuohnTk8IvNOxcCHvm",
      "XSdG-YFRHnYVk7kI-06Vq$$2HESC1OUPR-TICDuoZhzA"
    )
    val df_src_dep_csv = src_dep_csv(context).interim(
      "graph",
      "OQLLMaq2rPSEWmOyKwgyi$$r4qBTysyZCey-9mtcCQqP",
      "rp0ukw0af76_LaO2wXQTZ$$mTMxJPNDnZ0Vs1CLuJ-zH"
    )
    val df_deduplicate_by_first_name =
      deduplicate_by_first_name(context, df_src_dep_csv).interim(
        "graph",
        "2r3WBpGK4GFb51Foqc1uH$$b9820U5wbBHfbM6UJChGd",
        "LdIu_n-1EPUw24J0F61v4$$fyL_xp-FYFqtql-e6k1cw"
      )
    val df_src_snow_configs = src_snow_configs(context).interim(
      "graph",
      "_O1R11yhxHSXU-G6tp6fp$$zd5fMGJPvyry2M8jDMBtR",
      "UqOS_oKXg-BZTCh-_W7pX$$ZlKaUGXOu9lrPgaqtr61U"
    )
    val df_Limit_6 = Limit_6(context, df_src_snow_configs).interim(
      "graph",
      "aPkHdB8XMh1LfcF4Xj2GD$$JIJ5TJxe_kWx1nWwTWs8h",
      "5McB4Oxo0g2KeyQvfBdz0$$NQG1O06qOSG9ZJlOLU7os"
    )
    val df_Repartition_1 = Repartition_1(context, df_Limit_1).interim(
      "graph",
      "kJAOtV1RFjxZeLK84vqPK$$H98iCD4GLUcv0SRgUaImJ",
      "VUWVX_j59z-1akU9F6Yd_$$5PPFWUj1xyekNB--AMp7Z"
    )
    val df_custom_join_with_hints =
      custom_join_with_hints(context,
                             df_CustomReformat_1,
                             df_Repartition_1,
                             df_deduplicate_by_first_name,
                             df_Reformat_1,
                             df_Limit_4,
                             df_Limit_5,
                             df_Limit_6
      ).interim("graph",
                "m8yl20C97fjI9e_8JsLx7$$90v3zZTD3UqTQn0tX8eC7",
                "2FvkwcnWThg-GAgj-eUOO$$APboymzqWL-B5bSsEMkbA"
      )
    val df_aggregate_by_c_struct =
      aggregate_by_c_struct(context, df_custom_join_with_hints).interim(
        "graph",
        "nWZcN4rZPx5eOL-zDiEFC$$juf4AQGTrwKgye1Dim5zf",
        "Bn6NZV1LpzzONG9vdYWh5$$-psSV1ufpdIChutuD2wDw"
      )
    val (df_row_filter_distribution_out0, df_row_filter_distribution_out1) = {
      val (df_row_filter_distribution_out0_temp,
           df_row_filter_distribution_out1_temp
      ) = row_filter_distribution(context, df_aggregate_by_c_struct)
      (df_row_filter_distribution_out0_temp.interim(
         "graph",
         "3siAucNZZoCh-NYvbm7QS$$z1pYm5vAiKRH7wYqlGdrP",
         "-LJyBitqkRdcd59Bw0sx0$$SLXRMcKBAYG9vOkBWn-rw"
       ),
       df_row_filter_distribution_out1_temp.interim(
         "graph",
         "3siAucNZZoCh-NYvbm7QS$$z1pYm5vAiKRH7wYqlGdrP",
         "qE4xXig7v8S38qU4ibmQE$$X4dh4kXx3IDizi8MpfE8b"
       )
      )
    }
    val df_join_temp_views = join_temp_views(context,
                                             df_row_filter_distribution_out0,
                                             df_row_filter_distribution_out1
    ).interim("graph",
              "_fH0b4uoBEY742hezO1uT$$OSZhVFI43N1cqVHKuIFg5",
              "gdP5NP0Htjqyb1Ks2haVa$$8lYMTK9GCLpFgmGviU0my"
    )
    val df_src_avro_CustsDatasetInput_1 =
      src_avro_CustsDatasetInput_1(context).interim(
        "graph",
        "Tt0Dx7-Ez3xaS_Zk0rmm7$$MCnkvhOpEJ15f5dgrWX47",
        "gk-mfGIJXIecyoa3oW40l$$uNqQwD2Vo2FTsH-Cbc5gQ"
      )
    val df_filter_and_union_customers = filter_and_union_customers(
      context,
      df_src_avro_CustsDatasetInput_1,
      df_src_avro_CustsDatasetInput_1,
      df_src_avro_CustsDatasetInput_1,
      df_src_json_input_custs_1,
      df_src_json_input_custs_1,
      df_src_json_input_custs_1
    ).interim("graph",
              "qThV257C9HnSgBU3aaK5M$$uHREHknfEhc8Xez68tEDR",
              "kxpyaE6mtJoOwT54SgZGH$$l8q7bX1eMBgdzesxfH9Js"
    )
    val df_expressions_sg = expressions_sg.apply(
      expressions_sg.config
        .Context(context.spark, context.config.expressions_sg),
      df_src_json_input_custs_1
    )
    val df_identity_transformation =
      identity_transformation(context,
                              df_join_temp_views,
                              df_filter_and_union_customers,
                              df_Reformat_11_1,
                              df_expressions_sg
      ).interim("graph",
                "Fp18pTcRDk6UrZ9J2Nd7g$$4xPr7XJN4aw4_4GiikDz1",
                "Ajgn796fH1EkzxfsRKVcV$$thKnhfkdyIRRtJn_4HdEE"
      )
    val df_UNIT_TESTS_REFORMAT_GEM =
      UNIT_TESTS_REFORMAT_GEM(context, df_src_csv_special_char).interim(
        "graph",
        "hxT8l69zffXuREzW1_qxS$$WeWd0H45ReziLQFnxuqiw",
        "rmK2EW187QKpmEf_ej8gv$$VvPoBSnSLaYQ3NChU5CkS"
      )
    df_UNIT_TESTS_REFORMAT_GEM.cache().count()
    df_UNIT_TESTS_REFORMAT_GEM.unpersist()
    val df_SetOperation_1 = SetOperation_1(context,
                                           df_identity_transformation,
                                           df_identity_transformation
    ).interim("graph",
              "2fgfqdPypWizzs8saEWlK$$Yht3qM6JZCl0KOaHjQbQd",
              "-5kbFncFwViqg2FraiuyN$$V7wm5S50bucry7hZLV2Ys"
    )
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_medium_pipeline", context.spark) {
        dest_medium_pipeline(context, df_SetOperation_1)
      }
    }
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
                   "pipelines/Cloned_Medium_Pipeline_1"
    )
    spark.conf.set("spark_config1", "spark_config_value_1")
    spark.conf.set("spark_config2", "spark_config_value_2")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config_value2")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/Cloned_Medium_Pipeline_1") {
      spark.withSparkOptimisationsDisabled(graph(context))
    }
  }

}
