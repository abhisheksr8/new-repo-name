package io.prophecy.pipelines.medium_pipeline.graph.reformatted_columns_1

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
class Reformat_3Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/reformatted_columns_1/Reformat_3/in/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/reformatted_columns_1/Reformat_3/in/data/unit_test_.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/medium_pipeline/graph/reformatted_columns_1/Reformat_3/out/schema.json",
      "/data/io/prophecy/pipelines/medium_pipeline/graph/reformatted_columns_1/Reformat_3/out/data/unit_test_.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.medium_pipeline.graph.reformatted_columns_1
        .Reformat_3(
          io.prophecy.pipelines.medium_pipeline.graph.reformatted_columns_1.config
            .Context(context.spark, context.config.reformatted_columns_1),
          dfIn
        )
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
        "c_test_lookup_in_sg"
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
        "c_test_lookup_in_sg"
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
