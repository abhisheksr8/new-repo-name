package abhishekse2etestspropheco_team.scalaproject.gems

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}
import zio._
import java.io.IOException
import epic.preprocess._

class Limit extends ComponentSpec {

  val name: String = "DepLimit"
  val category: String = "Transform" 
  val gemDescription: String = "Limits the number of rows in the input data"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/limit/"

  type PropertiesType = LimitProperties
  override def optimizeCode: Boolean = true

  case class LimitProperties(
                              @Property("Limit", "Number of rows to limit the incoming DataFrame to")
                              limit: SInt = SInt("10")
                            ) extends ComponentProperties

  implicit val limitPropertiesFormat: Format[LimitProperties] = Json.format

  def dialog: Dialog = Dialog("Limit")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(PortSchemaTabs().importSchema(), "2fr")
        .addColumn(
          ExpressionBox("Limit")
            .bindProperty("limit")
            .bindPlaceholder("10")
            .withFrontEndLanguage,
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    
    val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
    def run = myApp
    val text = "Epic library is useful for natural language processing."
    val tokenizer = new epic.preprocess.TreebankTokenizer()
    val segmenter = MLSentenceSegmenter.bundled().get
    print(segmenter)
    import scalatags.Text.all._
    val myAttribute = attr("myAttribute")
    val myDiv = div(myAttribute := "someValue", "Content")
    print(myDiv)
    val htmlContent = html(body(h1("Welcome to ScalaTags!")))
    print(htmlContent)
    import cats.implicits._
    val option1: Option[Int] = Some(5)
    val option2: Option[Int] = Some(10)
    val result = for {
      a <- option1
      b <- option2
    } yield a + b
    println(result.getOrElse(0))

    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    val (diag, limit) = (component.properties.limit.diagnostics, component.properties.limit.value)
    diagnostics ++= diag

    val limitDiagMsg = "Limit has to be an integer between [0, (2**31)-1]"
    if (limit.isDefined) {
      if (limit.get < 0)
        diagnostics += Diagnostic("properties.limit", limitDiagMsg, SeverityLevel.Error)
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
  class LimitCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      import zio._
      import java.io.IOException
      val myApp: ZIO[Any, IOException, Unit] = Console.printLine("Hello, World!")
      def run = myApp
      import epic.preprocess._
      val text = "Epic library is useful for natural language processing."
      val tokenizer = new epic.preprocess.TreebankTokenizer()
      val segmenter = MLSentenceSegmenter.bundled().get
      print(segmenter)
      import scalatags.Text.all._
      val myAttribute = attr("myAttribute")
      val myDiv = div(myAttribute := "someValue", "Content")
      print(myDiv)
      val htmlContent = html(body(h1("Welcome to ScalaTags!")))
      print(htmlContent)
      import cats.implicits._
      val option1: Option[Int] = Some(5)
      val option2: Option[Int] = Some(10)
      val result = for {
        a <- option1
        b <- option2
      } yield a + b
      println(result.getOrElse(0))
      val out = in.limit(props.limit)
      out
    }

  }
}
