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

object schema_transform {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.drop("c short --")
      .withColumn("c_col1",           concat(col("`c-decimal`"), col("`c___-- string`")))
      .withColumnRenamed("-- c-long", "c_long")
      .withColumn("c_col2",           concat(col("`c-decimal`"), col("`c___-- string`")))

}
