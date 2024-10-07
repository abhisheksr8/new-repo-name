package io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.Subgraph_2.config._
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.Subgraph_2.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_5 = Reformat_5(context, in0).interim(
      "Subgraph_2",
      "hbK2V0KBaU7e2z4vsXez1$$x27yrC0gxxjcsyGSjsfip",
      "yYGWBCbdFpmi26Cp11ufi$$GVWd4_sYP-EQUi5m-dVFU"
    )
    df_Reformat_5
  }

}
