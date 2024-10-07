package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.config._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object pm_shared_graph {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_src_parquet_all_type_no_partition =
      src_parquet_all_type_no_partition(context).interim(
        "pm_shared_graph",
        "-To_u_DllLoz_BkfbJfUS$$bo-r9JTza9znlXD8oO6NI",
        "RmGL9u9DCrLlBi9aTDi9r$$eMC5YLgTo7OdZ7dtZUnLl"
      )
    val df_reformat_data =
      reformat_data(context, df_src_parquet_all_type_no_partition).interim(
        "pm_shared_graph",
        "0H8hV0JC6i0ZYzr43GMOT$$eTLxLf7J2QEtx-qd5k3Uo",
        "y0pgf5saGORVDng2ixbJm$$zN0T4sCz5ElGG-whmRl3U"
      )
    df_reformat_data.cache().count()
    df_reformat_data.unpersist()
    val df_Reformat_10_1 = Reformat_10_1(context, in0).interim(
      "pm_shared_graph",
      "Ly2IBGE1CPgoC51992Ii-$$R7ApmnowMxVXya3zmK85V",
      "dbBBb_rXY6Y3Tjj_5R8FF$$QCCQNQndxEvj3KxDNte45"
    )
    val df_deduplicate_by_first_name_1 =
      deduplicate_by_first_name_1(context, df_Reformat_10_1).interim(
        "pm_shared_graph",
        "8L5gE9UXhEUzs_g4yxjeX$$0krw1wkRwc4nyso1f0Lgg",
        "AyTL07DfqJajDcjV3MWxS$$TyQZQQdU15W49NweLwp0k"
      )
    df_deduplicate_by_first_name_1.cache().count()
    df_deduplicate_by_first_name_1.unpersist()
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_Reformat_10_1
    )
    val df_src_config_csv = src_config_csv(context).interim(
      "pm_shared_graph",
      "b_tmfs0tn5u9bRng8QvCE$$LxvMITn9zWlDu9oUtjHXp",
      "eHsLmFcs3EuVXkhPq8r_c$$qo2LjWyKx9oJCVHfGA5yo"
    )
    val df_Reformat_1 = Reformat_1(context, df_src_config_csv).interim(
      "pm_shared_graph",
      "8aE4-919ghq1tub5nQWe4$$5wpqop0xcvHqva5oIoULD",
      "wB7vEfCgX1ASlEvu3VlCY$$mh1P6Lvdr7h2VosVeDkJX"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    df_Subgraph_1
  }

}
