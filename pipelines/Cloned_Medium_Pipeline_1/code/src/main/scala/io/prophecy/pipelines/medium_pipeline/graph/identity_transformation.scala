package io.prophecy.pipelines.medium_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.config.Context
import io.prophecy.pipelines.medium_pipeline.udfs.UDFs._
import io.prophecy.pipelines.medium_pipeline.udfs.Rules._
import io.prophecy.pipelines.medium_pipeline.udfs.ColumnFunctions._
import io.prophecy.pipelines.medium_pipeline.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object identity_transformation {
  def apply(context: Context, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    def isPrime(n: Int): Boolean = {
      if (n <= 1) false
      else if (n == 2) true
      else !(2 to math.sqrt(n).toInt).exists(i => n % i == 0)
    }
    
    println(isPrime(5))  // true
    println(isPrime(10)) // false
    var a=Config.c_1*Config.c_0
    
    var o0 = in0.select(in0("first_name").alias("c_string_new"))
    var o1 = in1.select(in1("first_name").alias("c_string_new"))
    var o2 = in2.select(in2("first_name").alias("c_string_new"))
    var o3 = in3.select(in3("first_name").alias("c_string_new"))
    var out0=o0.union(o1).union(o2).union(o3)
    out0
  }

}
