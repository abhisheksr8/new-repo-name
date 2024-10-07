package io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(
  Subgraph_1:    Subgraph_1_Config = Subgraph_1_Config(),
  var1:          Int = 20,
  JDBC_DATABASE: String = "test_database",
  CSV_DATASET_LOCATION: String =
    "dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
