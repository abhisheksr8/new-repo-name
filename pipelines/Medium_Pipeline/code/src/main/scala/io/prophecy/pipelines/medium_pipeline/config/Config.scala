package io.prophecy.pipelines.medium_pipeline.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.reformatted_columns_1.config.{
  Config => reformatted_columns_1_Config
}
import io.prophecy.pipelines.medium_pipeline.graph.SubGraph_1.config.{
  Config => SubGraph_1_Config
}
import io.prophecy.pipelines.medium_pipeline.graph.subgraph_reformat_5.config.{
  Config => subgraph_reformat_5_Config
}
import io.prophecy.pipelines.medium_pipeline.graph.Subgraph_4.config.{
  Config => Subgraph_4_Config
}
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.config.{
  Config => pm_shared_graph_Config
}
import io.prophecy.pipelines.medium_pipeline.graph.expressions_sg.config.{
  Config => expressions_sg_Config
}

case class Config(
  @Description(
    "test_str this is randome description for this field @!#$!@$#$^$%*^(*(_()+&#$%%#~!@"
  ) c_test:                               Option[String] = None,
  @Description("array desc") c_array:     Option[List[String]] = None,
  @Description("record ddesc") c_record3: Option[C_record3] = None,
  @Description("bool desc") bool:         Boolean = true,
  @Description("double desc") double:     Double = 234324.0d,
  @Description("sewr") record_array:      Record_array = Record_array(),
  @Description("sdf") array_record: List[Array_record] = List(
    Array_record(werw = "332", ewr = List("12", "w"))
  ),
  reformatted_columns_1: reformatted_columns_1_Config =
    reformatted_columns_1_Config(),
  Subgraph_4: Subgraph_4_Config = Subgraph_4_Config(),
  @Description(
    "main hoon subgraph 1"
  ) subgraph_reformat_5: subgraph_reformat_5_Config =
    subgraph_reformat_5_Config(),
  c_int:              Int = 1,
  c_string:           String = "SCALA_BASIC - DEFFAULT",
  c_spark_expression: String = "concat('a', 'b')",
  SubGraph_1:         SubGraph_1_Config = SubGraph_1_Config(),
  pm_shared_graph:    pm_shared_graph_Config = pm_shared_graph_Config(),
  SNOW_PASSWORD:      String = "CuIqZ!9I32t@",
  SNOW_USERNAME:      String = "cicdaccount",
  c_dedup_expr:       String = "concat(c_array_float, `c_array_int`)",
  c_dedup_col:        String = " c_array_date",
  c_st_expr:          String = "concat(`c_struct-c_short`, `c_struct-c_decimal`)",
  c_st_long:          String = "c_array_long",
  c_st_rename:        String = "c_array_boolean_renamed",
  c_1:                Int = 1,
  c_0:                Long = 0L,
  expressions_sg:     expressions_sg_Config = expressions_sg_Config(),
  config_1:           String = "test1",
  config_2:           String = "test2",
  config_3:           String = "test2",
  config_4:           String = "test2",
  config_5:           String = "test2",
  config_6:           String = "test2",
  config_7:           String = "test2",
  config_8:           String = "test2",
  config_9:           String = "test2test2",
  config_10:          String = "test2",
  config_11:          String = "test2",
  config_12:          String = "test2",
  config_13:          String = "test2",
  config_14:          String = "test2",
  config_15:          String = "test2",
  config_16:          String = "test2",
  config_17:          String = "test2",
  config_18:          String = "test2",
  config_19:          String = "test2",
  config_20:          String = "test2"
) extends ConfigBase

object C_record3 {

  implicit val confHint: ProductHint[C_record3] =
    ProductHint[C_record3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_record3(
  @Description("c value record") c_val3: Option[C_val3] = None
)

object C_val3 {

  implicit val confHint: ProductHint[C_val3] =
    ProductHint[C_val3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_val3(@Description("crr desc") crr: Option[String] = None)

object Record_array {

  implicit val confHint: ProductHint[Record_array] =
    ProductHint[Record_array](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Record_array(@Description("wer") array: List[String] = List("23"))

object Array_record {

  implicit val confHint: ProductHint[Array_record] =
    ProductHint[Array_record](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Array_record(werw: String, ewr: List[String])
