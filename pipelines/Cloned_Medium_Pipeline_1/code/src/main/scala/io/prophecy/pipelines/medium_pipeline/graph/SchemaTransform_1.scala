package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.medium_pipeline.udfs.UDFs._
import io.prophecy.pipelines.medium_pipeline.udfs.Rules._
import io.prophecy.pipelines.medium_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.medium_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SchemaTransform_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.withColumn("c_concat_new_short_decimal", expr(Config.c_st_expr))
      .drop(Config.c_st_long)
      .withColumnRenamed("c_array_boolean", Config.c_st_rename)
  }

}
