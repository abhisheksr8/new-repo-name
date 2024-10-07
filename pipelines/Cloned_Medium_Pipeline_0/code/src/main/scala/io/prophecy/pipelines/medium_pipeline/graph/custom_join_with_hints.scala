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

object custom_join_with_hints {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame,
    in6:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .hint("broadcast")
      .join(in1.as("in1").hint("broadcast"),
            col("in0.c_int") =!= col("in1.c_array_int"),
            "outer"
      )
      .join(in2.as("in2").hint("broadcast"),
            col("in1.c_array_string") =!= col("in2.first_name"),
            "left_outer"
      )
      .join(in3.as("in3").hint("broadcast"),
            col("in2.first_name") =!= col("in3.`c___-- string`"),
            "inner"
      )
      .join(in4.as("in4").hint("broadcast"),
            col("in3.`c___-- string`") =!= col("in4.`c   short  --`"),
            "right_outer"
      )
      .join(in5.as("in5").hint("broadcast"),
            col("in4.`c   short  --`") =!= col("in5.`c   short  --`"),
            "outer"
      )
      .join(in6.as("in6").hint("broadcast"),
            col("in5.`c   short  --`") =!= col("in6.C_VARCHAR"),
            "inner"
      )
      .select(
        col("in0.c_int").as("c_int"),
        col("in1.`c_struct-c_timestamp`").as("c_struct-c_timestamp"),
        col("in2.customer_id").as("customer_id"),
        col("in2.first_name").as("first_name"),
        col("in3.`c-int-column type`").as("c-int-column type"),
        col("in4.`c   short  --`").as("c   short  --"),
        col("in5.`c  float`").as("c  float"),
        col("in5.`c-decimal`").as("c-decimal"),
        col("in6.C_TIME").as("C_TIME"),
        col("in6.C_ARRAY").as("C_ARRAY"),
        col("in6.C_OBJECT").as("C_OBJECT"),
        col("in6.C_BOOL").as("C_BOOL")
      )

}
