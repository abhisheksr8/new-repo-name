package io.prophecy.pipelines.very_small_pipeline.udfs

import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import java.time._

object Rules extends Serializable {

  def br_src_parquet_all_type_and_partition_withspacehyphens(
    c__short:                   Column = col("`c- short`"),
    c____int:                   Column = col("`c  - int`"),
    c_float_____               : Column = col("`c_float-__  `"),
    c_string:                   Column = col("`c-string`"),
    c_date_for_today:           Column = col("`c_date-for today`"),
    c_timestamp_____for__today: Column = col("`c_timestamp  __ for--today`"),
    c_array_string____string:   Column = col("`c_array-string  _ string`"),
    c_struct_______            : Column = col("`c_struct -- _  `"),
    c____boolean____           : Column = col("`c -  boolean _  `"),
    c_decimal_____             : Column = col("`c_decimal  -  `")
  ): Column =
    when(
      (c__short === lit(10))
        .and(c____int.isNotNull)
        .and((c_float_____ < lit(0)).and(c_string.like("%9%")))
        .and(
          (c_date_for_today === lit("is null").cast(DateType))
            .and(
              c_timestamp_____for__today === to_timestamp(
                lit("2001-10-13 10:20:44")
              )
            )
            .and(
              !(c____boolean____ <=> lit(true))
                .and(c_decimal_____ === lit(12321))
            )
        ),
      lit(1)
    ).when(c_float_____.isNotNull
              .and(c_date_for_today =!= lit("is null").cast(DateType))
              .and(c_decimal_____ > lit(12321)),
            lit(2)
      )
      .when(
        (c__short > lit(10))
          .and(!c_string.like("%4%"))
          .and(
            c_timestamp_____for__today =!= to_timestamp(
              lit("2001-10-13 10:20:44")
            )
          )
          .and(
            !(c____boolean____ <=> lit(false)).and(c_decimal_____ <= lit(12321))
          ),
        lit(3)
      )
      .when((c__short < lit(20)).and(c_string.like("%7%")), lit(4))
      .when((c____int > lit(22))
              .and(c_string.like("%6%"))
              .and(c_date_for_today.isNotNull.and(c____boolean____.isNotNull)),
            lit(5)
      )
      .when(
        c____int
          .isin(6540, 6541)
          .and(c_string.isin("r#$%@#4!*&^()_=ASD~!405"))
          .and(
            c_timestamp_____for__today.isNotNull
              .and(c____boolean____.isin(true, false))
          ),
        lit(6)
      )
      .when((c_float_____ > lit(0)).and(c_array_string____string.isNotNull),
            lit(7)
      )
      .when(c_struct_______.isNotNull, lit(8))
      .when(c__short.isNotNull,        lit(9))
      .when(
        c__short.isNull
          .and(c____int.isNull)
          .and(c_float_____.isNull.and(c_string.isNull))
          .and(
            c_date_for_today.isNull
              .and(c_timestamp_____for__today.isNull)
              .and(c_array_string____string.isNull.and(c_struct_______.isNull))
          ),
        lit(10)
      )
      .otherwise(lit(22))
      .as("br_src_parquet_all_type_rule")

  def br_src_parquet_all_type_and_partition_withspacehyphens_basic(
    c_double: Column = col("c_double"),
    c_string: Column = col("`c-string`")
  ): Column =
    when((c_double > lit(45730)).and(c_string.like("%6%")), lit("Valid"))
      .when((c_double > lit(34)).and(c_string.like("%9%")), lit("Invalid"))
      .otherwise(lit("Other"))
      .as("br_basic_parquet")

}
