package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.Subgraph_4.Subgraph_5
import io.prophecy.pipelines.medium_pipeline.graph.Subgraph_4.config._
import io.prophecy.pipelines.medium_pipeline.graph.Subgraph_4.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_8 = Reformat_8(context, in0).interim(
      "Subgraph_4",
      "N5fCkDqLuqNf6isccevNa$$Vd7N6QM4EKh32Eo9d-hES",
      "KCpXTET0p3nqsi-30JBQX$$DXFDvxmcZD89bpnllrDsQ"
    )
    val df_Subgraph_5 = Subgraph_5.apply(
      Subgraph_5.config.Context(context.spark, context.config.Subgraph_5),
      df_Reformat_8
    )
    df_Subgraph_5
  }

}
