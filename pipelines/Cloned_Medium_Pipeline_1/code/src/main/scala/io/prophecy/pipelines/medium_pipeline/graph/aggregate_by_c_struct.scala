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

object aggregate_by_c_struct {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("C_BOOL"))
      .agg(
        first(col("first_name")).as("first_name"),
        first(col("`c_struct-c_timestamp`")).as("c_struct-c_timestamp"),
        first(col("customer_id")).as("customer_id"),
        first(col("`c-int-column type`")).as("c-int-column type"),
        first(col("`c  float`")).as("c  float"),
        first(col("`c-decimal`")).as("c-decimal"),
        first(col("C_TIME")).as("C_TIME"),
        first(col("C_ARRAY")).as("C_ARRAY"),
        first(col("C_OBJECT")).as("C_OBJECT")
      )

}
