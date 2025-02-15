package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_orc_all_type_no_partition {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("orc")
      .load("dbfs:/Prophecy/qa_data/orc/all_type_no_partition")

}
