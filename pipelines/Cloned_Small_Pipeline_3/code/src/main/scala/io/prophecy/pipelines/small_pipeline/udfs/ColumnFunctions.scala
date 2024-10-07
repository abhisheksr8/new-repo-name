package io.prophecy.pipelines.small_pipeline.udfs

import io.prophecy.pipelines.small_pipeline.udfs.UDFs._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import java.time._

object ColumnFunctions extends Serializable {

  def col_func_all_type_param_scala(
    c_col:          Column,
    c_array_string: Column,
    c_binary:       Column,
    c_boolean:      Column,
    c_date:         Column,
    c_double:       Column,
    c_float:        Column,
    c_int:          Column,
    c_long:         Column,
    c_short:        Column,
    c_string:       Column,
    c_struct:       Column,
    c_timestamp:    Column,
    c_decimal:      Column
  ): Column =
    concat(c_string,
           c_int,
           c_col,
           c_array_string(0),
           c_struct.getField("c_s_string"),
           c_int,
           c_struct.getField("c_s_array_int")(0)
    )

  def col_func_all_type_param_sql(
    c_col:          Column,
    c_array_string: Column,
    c_binary:       Column,
    c_boolean:      Column,
    c_date:         Column,
    c_double:       Column,
    c_float:        Column,
    c_int:          Column,
    c_long:         Column,
    c_short:        Column,
    c_string:       Column,
    c_struct:       Column,
    c_timestamp:    Column,
    c_decimal:      Column
  ): Column =
    concat(
      c_col,
      c_boolean,
      c_struct.getField("c_s_array_int").getItem(0),
      c_array_string(0),
      (lit("22").cast(IntegerType) > lit(5))
        .and(lit("22").cast(IntegerType) =!= lit(0))
        .or(c_string.like("%A%"))
    )

}
