package io.prophecy.pipelines.medium_pipeline.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.medium_pipeline.config._
import io.prophecy.libs.registerAllUDFs
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class aggregate_by_c_structTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/aggregate_by_c_struct/in/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/aggregate_by_c_struct/in/data/unit_test_.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/aggregate_by_c_struct/out/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/aggregate_by_c_struct/out/data/unit_test_.json",
      "out"
    )

    val dfOutComputed = io.prophecy.pipelines.medium_pipeline.graph
      .aggregate_by_c_struct(context, dfIn)
    val res = assertDFEquals(
      dfOut.select("C_BOOL",
                   "first_name",
                   "c_struct-c_timestamp",
                   "customer_id",
                   "c-int-column type",
                   "c  float",
                   "c-decimal",
                   "C_TIME",
                   "C_ARRAY",
                   "C_OBJECT"
      ),
      dfOutComputed.select("C_BOOL",
                           "first_name",
                           "c_struct-c_timestamp",
                           "customer_id",
                           "c-int-column type",
                           "c  float",
                           "c-decimal",
                           "C_TIME",
                           "C_ARRAY",
                           "C_OBJECT"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerAllUDFs(spark)

    val fabricName = System.getProperty("fabric", "default")
    val confFilePath = Paths
      .get(getClass.getResource(s"/medium_pipeline/${fabricName}.json").toURI)
      .toString

    val config =
      ConfigurationFactoryImpl.getConfig(Array("--confFile", confFilePath))

    context = Context(spark, config)

    val dfProphecy_pipelines_medium_pipeline_graph_Lookup_1 =
      createDfFromResourceFiles(
        spark,
        "/data/io/prophecy/pipelines/medium_pipeline/graph/Lookup_1/schema.json",
        "/data/io/prophecy/pipelines/medium_pipeline/graph/Lookup_1/data.json",
        port = "in"
      )
    io.prophecy.pipelines.medium_pipeline.graph
      .Lookup_1(context, dfProphecy_pipelines_medium_pipeline_graph_Lookup_1)
  }

}
