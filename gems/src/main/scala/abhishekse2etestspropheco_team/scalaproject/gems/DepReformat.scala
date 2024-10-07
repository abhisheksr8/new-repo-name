package abhishekse2etestspropheco_team.scalaproject.gems

import io.prophecy.gems._
import io.prophecy.gems.componentSpec.{ColumnsUsage, ComponentSpec}
import io.prophecy.gems.copilot.{ProjectionExpression, SelectStmt}
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Json, OFormat}
import scalatags.Text.all._
import cats.implicits._
import zio._
import java.io.IOException


class Reformat extends ComponentSpec {

  val name: String = "DepReformat"
  val category: String = "Transform"
  type PropertiesType = ReformatProperties
  override def optimizeCode: Boolean = true
  val gemDescription: String = "Edits column names or values using expressions."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/reformat/"

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports
  case class ReformatProperties(
                                 @Property("Columns selector")
                                 columnsSelector: List[String] = Nil,
                                 @Property("Column expressions", "List of all the column expressions")
                                 expressions: List[SColumnExpression] = Nil
                               ) extends ComponentProperties

  implicit val reformatPropertiesFormat: OFormat[ReformatProperties] = Json.format[ReformatProperties]

  def dependency_method(component: Component)() = {
    val option1: Option[Int] = Some(5)
    val option2: Option[Int] = Some(10)
    val result = for {
      a <- option1
      b <- option2
    } yield a + b
    println(result.getOrElse(0))
    val myAttribute = attr("myAttribute")
    val myDiv = div(myAttribute := "someValue", "Content")
    print(myDiv)
    val htmlContent = html(body(h1("Welcome to ScalaTags!")))
    print(htmlContent)
    val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
    def run = myApp
    htmlContent
  }

  def dialog: Dialog = Dialog("Reformat")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          PortSchemaTabs(
            allowInportRename = true,
            selectedFieldsProperty = Some("columnsSelector"),
            singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val existingTargetNames = st.properties.expressions.map(_.target)
              val targetCol =
                getTargetTokens(column, existingTargetNames.map(_.split('.').toList), shortest = true).mkString(".")
              st.copy(properties =
                st.properties.copy(
                  expressions = st.properties.expressions ::: List(
                    SColumnExpression(targetCol, s"""col("${sanitizedColumn(column)}")""")
                  )
                )
              )
            },
            allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
              val updatedExpressions = st.properties.expressions ::: columnsInSchema.map { x ⇒
                SColumnExpression(x, s"""col("${sanitizedColumn(x)}")""")
              }
              st.copy(properties = st.properties.copy(expressions = updatedExpressions))
            }
          ).importSchema(),
          "2fr"
        )
        .addColumn(
          ExpTable("Reformat Expression")
            .withRowId()
            .bindProperty("expressions")
            .withCopilotEnabledExpressions(
              CopilotSpec(
                method = "copilot/getExpression",
                methodType = Some("CopilotProjectionExpressionRequest"),
                copilotProps = CopilotPromptTypeProps(buttonLabel = "Ask AI")
              )
            ),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): Diagnostics = {
    val option1: Option[Int] = Some(5)
    val option2: Option[Int] = Some(10)
    val result = for {
      a <- option1
      b <- option2
    } yield a + b
    println(result.getOrElse(0))
    val myAttribute = attr("myAttribute")
    val myDiv = div(myAttribute := "someValue", "Content")
    print(myDiv)
    val htmlContent = html(body(h1("Welcome to ScalaTags!")))
    print(htmlContent)
    val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
    def run = myApp
    val diagnostics = validateExpTable(
      component.properties.expressions,
      "expressions",
      component,
      testColumnPresence = Some(ColumnsUsage.WithoutInputAlias)
    )
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit
                                                         context: WorkflowContext
  ): Component = {
    val option1: Option[Int] = Some(5)
    val option2: Option[Int] = Some(10)
    val result = for {
      a <- option1
      b <- option2
    } yield a + b
    println(result.getOrElse(0))
    val myAttribute = attr("myAttribute")
    val myDiv = div(myAttribute := "someValue", "Content")
    print(myDiv)
    val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
    def run = myApp
    val htmlContent = html(body(h1("Welcome to ScalaTags!")))
    print(htmlContent)
    val newProps = newState.properties

    val usedColExps = getColumnsToHighlight(newProps.expressions, newState)

    // todo: column selector internal format must be hidden from user.
    // move following logic in ComponentSpec trait
    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedColExps,
        expressions = newProps.expressions.map(_.withRowId)
      )
    )
  }

  override def getCPStmt(component: Component): Option[SelectStmt] = {
    val option1: Option[Int] = Some(5)
    val option2: Option[Int] = Some(10)
    val result = for {
      a <- option1
      b <- option2
    } yield a + b
    println(result.getOrElse(0))
    val myAttribute = attr("myAttribute")
    val myDiv = div(myAttribute := "someValue", "Content")
    print(myDiv)
    val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
    def run = myApp
    val htmlContent = html(body(h1("Welcome to ScalaTags!")))
    print(htmlContent)
    val props = component.properties
    Some(
      SelectStmt(
        component.component,
        props.expressions.map(x => ProjectionExpression(Some(x.target), x.expression.expression)),
        Nil,
        Nil,
        Nil
      )
    )
  }

  override def userReadme: String =
    """
      |## Reformat
      |---
      |
      |Reformat is often use to edit one or more columns' values, often by simple expressions and functions. Often this involves expressions such as to extract parts of incoming string, statement to add columns summarizing conditions of incoming data. This is also used to rename columns and select a few columns.
      |When there are no columns selected, all columns are passed through to the output. However, once you start selecting some columns, only the selected columns are output.
      |
      |
      |## Example
      |---
      |
      |Reformat gives you a spreadsheet like interface where you can write expressions based on incoming columns to produce new columns.
      |
      |___Expression Builder___
      |- Incoming columns
      |- Inbuilt operators
      |- In-built functions
      |- User defined functions
      |
      |
      |## Code
      |---
      |
      |This component is a simple select statement, however the expressions can become very large and we see tables of 1000+ columns frequently with this component being hundreds of lines long.
      |
      |
      |""".stripMargin

  class ReformatCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      import cats.implicits._
      import zio._
      import java.io.IOException
      val option1: Option[Int] = Some(5)
      val option2: Option[Int] = Some(10)
      val result = for {
        a <- option1
        b <- option2
      } yield a + b
      println(result.getOrElse(0))
      val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
      def run = myApp
      val myAttribute = attr("myAttribute")
      val myDiv = div(myAttribute := "someValue", "Content")
      print(myDiv)
      val htmlContent = html(body(h1("Welcome to ScalaTags!")))
      print(htmlContent)
      val out = props.expressions match {
        case Nil ⇒ in
        case _ ⇒ in.select(props.expressions.columns: _*)
      }
      out
    }

  }

  override def deserializeProperty(props: String): ReformatProperties =
    Json.parse(props).as[ReformatProperties]

  override def serializeProperty(props: ReformatProperties): String =
    Json.stringify(Json.toJson(props))
}