package io.prophecy.pipelines.very_small_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.very_small_pipeline.graph.reformatted_columns_1.config._
import io.prophecy.pipelines.very_small_pipeline.graph.reformatted_columns_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object reformatted_columns_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_2 = Reformat_2(context, in0).interim(
      "reformatted_columns_1",
      "ugLA-pAzjs7ixVrEDX8tw$$17WaSsv-LLH4IuKrn27rs",
      "YruOL_Ib1eLyutfJsFwiq$$BzNg8UFjsr_zxjVAolssu"
    )
    val df_Reformat_3 = Reformat_3(context, df_Reformat_2).interim(
      "reformatted_columns_1",
      "7ACUG7bZD2YiBumZwdTsp$$ezAx1x3FF88JwAwdt_I55",
      "i_26utt2Trm87J0Qlv1yw$$7f6wRA1Cm_oTip84aYggZ"
    )
    df_Reformat_3
  }

}
