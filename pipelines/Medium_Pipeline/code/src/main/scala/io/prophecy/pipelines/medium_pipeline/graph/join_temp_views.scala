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

object join_temp_views {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    context.spark.sql(
      "select in0.first_name, in1.customer_id, in0.C_BOOL from in0,in1 where in0.C_BOOL==in1.C_BOOL limit 100"
    )
  }

}
