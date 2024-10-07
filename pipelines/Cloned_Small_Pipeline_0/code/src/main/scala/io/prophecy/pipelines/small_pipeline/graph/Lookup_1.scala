package io.prophecy.pipelines.small_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.small_pipeline.config.Context
import io.prophecy.pipelines.small_pipeline.udfs.UDFs._
import io.prophecy.pipelines.small_pipeline.udfs.Rules._
import io.prophecy.pipelines.small_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.small_pipeline.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_1 {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("TestLookup",
                 in0,
                 context.spark,
                 List("last_name", "customer_id"),
                 "first_name"
    )

}
