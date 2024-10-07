package io.prophecy.pipelines.very_small_pipeline.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.very_small_pipeline.graph.reformatted_columns_1.config.{
  Config => reformatted_columns_1_Config
}
import io.prophecy.pipelines.very_small_pipeline.graph.subgraph_reformat_5.config.{
  Config => subgraph_reformat_5_Config
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
  @Description(
    "main hoon subgraph 1"
  ) subgraph_reformat_5: subgraph_reformat_5_Config =
    subgraph_reformat_5_Config(),
  c_int:              Int = 1,
  c_string:           String = "SCALA_BASIC - DEFFAULT",
  c_spark_expression: String = "concat('a', 'b')"
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
