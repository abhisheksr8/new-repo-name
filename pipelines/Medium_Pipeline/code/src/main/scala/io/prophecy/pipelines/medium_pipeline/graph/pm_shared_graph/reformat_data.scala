package io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.medium_pipeline.udfs.UDFs._
import io.prophecy.pipelines.medium_pipeline.udfs.Rules._
import io.prophecy.pipelines.medium_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object reformat_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col_func_all_type_param_scala(
        col("c_short"),
        col("c_array_string"),
        lit("a").cast(BinaryType),
        lit(true),
        col("c_date"),
        col("c_double"),
        col("c_float"),
        col("c_int"),
        col("c_long"),
        col("c_short"),
        col("c_string"),
        expr(
          "named_struct('c_s_string', 'test', 'c_s_array_int', array(1, 2, 3, 4))"
        ),
        col("c_timestamp"),
        col("c_decimal")
      ).as("c_test"),
      col_func_all_type_param_sql(
        col("c_short"),
        col("c_array_string"),
        lit("a").cast(BinaryType),
        lit(true),
        col("c_date"),
        col("c_double"),
        col("c_float"),
        col("c_int"),
        col("c_long"),
        col("c_short"),
        col("c_string"),
        expr(
          "named_struct('c_s_string', 'test', 'c_s_array_int', array(1, 2, 3, 4))"
        ),
        col("c_timestamp"),
        col("c_decimal")
      ).as("c_test1")
    )

}
