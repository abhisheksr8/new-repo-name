package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.SubGraph_1.config._
import io.prophecy.pipelines.medium_pipeline.graph.SubGraph_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object SubGraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_9_1 = Reformat_9_1(context, in0).interim(
      "SubGraph_1",
      "Q-MaEwJY45ZD2iK8PuvmE$$x7H2dVWHuTM8HTgaXkUhJ",
      "RRPoKrNB5QR-c_sfPJUuD$$iKCYbkFeXAFvxeXUvp1XM"
    )
    df_Reformat_9_1
  }

}
