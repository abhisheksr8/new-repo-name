package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.Subgraph_2
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.config._
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object subgraph_reformat_5 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_2 = Subgraph_2.apply(
      Subgraph_2.config.Context(context.spark, context.config.Subgraph_2),
      in0
    )
    df_Subgraph_2
  }

}
