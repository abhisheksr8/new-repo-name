package io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.medium_pipeline.graph.pm_shared_graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_jdbc_mix_creds_1 {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var reader = context.spark.read.format("jdbc")
    reader = reader
      .option(
        "url",
        s"jdbc:mysql://${SecretManager
          .get("secrets/qa",        "mysql_host", "HashiCorp")}:${dbutils.secrets
          .get(scope = "qasecrets", key = "mysql_port")}/${context.config.JDBC_DATABASE}"
      )
      .option("user",
              s"${dbutils.secrets.get(scope = "qasecrets", key = "mysql_user")}"
      )
      .option(
        "password",
        s"${SecretManager.get("secrets/qa", "mysql_password", "HashiCorp")}"
      )
      .option("pushDownPredicate",  true)
      .option("driver",             "com.mysql.jdbc.Driver")
    reader = reader.option("query", "select * from test_table_automation")
    var df = reader.load()
    df
  }

}
