package io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1.config._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_11_1 = Reformat_11_1(context, in0).interim(
      "Subgraph_1",
      "scquc15pv5AvPVXgGyWJy$$xbaQwkYlfqJ1dm9CS-PXX",
      "QMcIIElLXTnydERZY6sjL$$iEuDj7d74bRKEA5-0GzgP"
    )
    val df_src_jdbc_mix_creds_1 = src_jdbc_mix_creds_1(context).interim(
      "Subgraph_1",
      "cnrKWOKwwC66GPLulpCtd$$TqDxZJbr3r-_CSqDOe_KC",
      "RvlDTPa7u3AoLkfpKXuND$$Z_GvJ3tazmd6RB6OWxzzP"
    )
    val df_Reformat_1_1 =
      Reformat_1_1(context, df_src_jdbc_mix_creds_1).interim(
        "Subgraph_1",
        "OH-JtFodOZE1E3mDeXyH3$$y-SDiBUr9GaThaJ2q6Nrr",
        "Yk6FoGmfY7yVEjniYgmgQ$$1XKCfQfpnsHAnORhwMM8G"
      )
    df_Reformat_1_1.cache().count()
    df_Reformat_1_1.unpersist()
    df_Reformat_11_1
  }

}
