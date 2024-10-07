package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_snow_configs {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    var reader = context.spark.read
      .format("snowflake")
      .options(
        Map(
          "sfUrl" -> "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
          "sfUser" -> (s"${Config.SNOW_USERNAME}"),
          "sfPassword" -> (s"${Config.SNOW_PASSWORD}"),
          "sfDatabase" -> "QA_DATABASE",
          "sfSchema" -> "QA_SCHEMA",
          "sfWarehouse" -> "COMPUTE_WH",
          "sfRole" -> ""
        )
      )
    reader =
      reader.option("query", "select * from all_type_table_smaller limit 10")
    reader.load()
  }

}
