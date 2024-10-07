package io.prophecy.pipelines.very_small_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.very_small_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.very_small_pipeline.udfs.UDFs._
import io.prophecy.pipelines.very_small_pipeline.udfs.Rules._
import io.prophecy.pipelines.very_small_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.very_small_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_11 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("`c   short  --`").as("c   short  --"),
              col("`c-int-column type`").as("c-int-column type"),
              col("`c--boolean`").as("c--boolean")
    )

}
