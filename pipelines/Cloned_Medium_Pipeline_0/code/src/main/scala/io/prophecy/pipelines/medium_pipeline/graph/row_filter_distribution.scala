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

object row_filter_distribution {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(
       col("C_TIME").isNotNull
         .and(col("C_BOOL").isin(true, false))
         .and(col("first_name").like("%b%"))
     ),
     in.filter(col("first_name").like("%a%"))
    )

}
