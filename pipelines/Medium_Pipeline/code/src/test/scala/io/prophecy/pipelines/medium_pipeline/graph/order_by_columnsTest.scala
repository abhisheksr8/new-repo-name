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
class order_by_columnsTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/order_by_columns/in/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/order_by_columns/in/data/unit_test_.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/order_by_columns/out/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/order_by_columns/out/data/unit_test_.json",
      "out"
    )

    val dfOutComputed = io.prophecy.pipelines.medium_pipeline.graph
      .order_by_columns(context, dfIn)
    val res = assertDFEquals(
      dfOut.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c_decimal"
      ),
      dfOutComputed.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c_decimal"
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
