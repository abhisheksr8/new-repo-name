package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.expressions_sg.config._
import io.prophecy.pipelines.medium_pipeline.graph.expressions_sg.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object expressions_sg {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_REF_DONOT_CHANGE = REF_DONOT_CHANGE(context, in0).interim(
      "expressions_sg",
      "TTOWjrnIg1fd9K_lp12Jl$$vEp5vXgDKnu2R2NPjxIZr",
      "WCBfclRvqMrrGgLXlPFSM$$ff67W4DfPBhO2HTji-buk"
    )
    val df_SQL_DONOT_CHANGE = SQL_DONOT_CHANGE(context, in0).interim(
      "expressions_sg",
      "kOIAa7KrZNtDA3rc__P61$$oLPYCu-sl9K1cSMJqf3tt",
      "VWxsvkKRBSLUAqVv_kOZ-$$IFfh5OnZnsfo-447oFm_k"
    )
    val df_Join_1 =
      Join_1(context, df_REF_DONOT_CHANGE, df_SQL_DONOT_CHANGE).interim(
        "expressions_sg",
        "n1lcDGx8VC5GRkw1L2mJe$$plh33zknNHEkHwvVujkMy",
        "2vrpj24FxoLGDEYCVRT3u$$w3vA3m1xHwi6jVbCq2J0X"
      )
    val df_Limit_1_1 = Limit_1_1(context, df_Join_1).interim(
      "expressions_sg",
      "4YHcNqQ_z_gOOILNqmSFP$$piAEDIyp-jcmnrxM90adU",
      "NGoJ34upvgON68LeHPWF9$$aehQXnseu9xJJ3SqLhAUW"
    )
    df_Limit_1_1
  }

}
