package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_catalog_table_test_catalog_source {

  def apply(context: Context): DataFrame =
    context.spark.read.table("`qa_database`.`test_catalog_source`")

}
