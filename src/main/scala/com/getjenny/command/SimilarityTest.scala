package com.getjenny.command

/**
  * Created by angelo on 11/04/17.
  */

import java.io.{File, FileReader, FileWriter}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import au.com.bytecode.opencsv.CSVWriter
import breeze.io.CSVReader
import com.getjenny.starchat.entities._
import com.getjenny.starchat.serializers.JsonSupport
import scopt.OptionParser

import scala.collection.immutable
import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object SimilarityTest extends JsonSupport {

  private[this] case class Params(
                            host: String = "http://localhost:8888",
                            indexName: String = "index_0",
                            path: String = "/analyzers_playground",
                            inputFile: String = "pairs.csv",
                            outputFile: String = "output.csv",
                            analyzer: String = "keyword(\"test\")",
                            itemList: Seq[String] = Seq.empty[String],
                            variables: Map[String, String] = Map.empty[String, String],
                            text1Index: Int = 3,
                            text2Index: Int = 4,
                            separator: Char = ',',
                            skipLines: Int = 1,
                            timeout: Int = 60,
                            headerKv: Seq[String] = Seq.empty[String]
                           )

  private[this] def execute(params: Params) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val skipLines = params.skipLines

    val baseUrl = params.host + "/" + params.indexName + params.path
    val file = new File(params.inputFile)
    val fileReader = new FileReader(file)
    lazy val termTextEntries = CSVReader.read(input=fileReader, separator=params.separator,
      quote = '"', skipLines=skipLines)

    val httpHeader: immutable.Seq[HttpHeader] = if(params.headerKv.nonEmpty) {
      val headers: Seq[RawHeader] = params.headerKv.map(x => {
        val headerOpt = x.split(":")
        val key = headerOpt(0)
        val value = headerOpt(1)
        RawHeader(key, value)
      }) ++ Seq(RawHeader("application", "json"))
      headers.to[immutable.Seq]
    } else {
      immutable.Seq(RawHeader("application", "json"))
    }

    val timeout = Duration(params.timeout, "s")

    val outFile = new File(params.outputFile)
    val fileWriter = new FileWriter(outFile)
    val outputCsv = new CSVWriter(fileWriter, params.separator, '"')

    termTextEntries.foreach(entry => {

      val text1 = entry(params.text1Index).toString
      val text2 = entry(params.text2Index).toString
      val escaped_text1 = text1.replace("\"", "\\\"")
      val escaped_text2 = text2.replace("\"", "\\\"")

      val analyzer: String =
        params.analyzer.replace("%text1", escaped_text1).replace("%text2", escaped_text2)
      val evaluate_request = AnalyzerEvaluateRequest(
        analyzer = analyzer,
        query = text2,
        data = Option{ AnalyzersData_v4(extracted_variables = params.variables,
          item_list = params.itemList.toVector) }
      )

      val entityFuture = Marshal(evaluate_request).to[MessageEntity]
      val entity = Await.result(entityFuture, 10.second)
      val responseFuture: Future[HttpResponse] =
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = baseUrl,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.OK =>
          val response =
            Unmarshal(result.entity).to[AnalyzerEvaluateResponse]
          val value = response.value match {
            case Some(evalResponse) => evalResponse match {
              case Success(t) => t
              case Failure(e) => AnalyzerEvaluateResponse(
                build = false, value = 0.0, build_message = "Failed evaluating response", data = None)
            }
            case _ =>
              AnalyzerEvaluateResponse(
                build = false, value = 0.0, build_message = "Response is empty", data = None)
          }
          val score = value.value.toString
          val input_csv_fields = entry.toArray
          val csv_line = input_csv_fields ++ Array(score)
          outputCsv.writeNext(csv_line)
        case _ =>
          println("failed running analyzer(" + evaluate_request.analyzer
            + ") Query(" + evaluate_request.query + ")")
      }
    })
    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("SimilarityTest") {
      head("Execute similarity test over a map of similar sentences." +
        " The text1 and text2 can be defined as a templates")
      help("help").text("prints this usage text")
      opt[String]("inputfile")
        .text(s"the path of the csv file with sentences" +
          s"  default: ${defaultParams.inputFile}")
        .action((x, c) => c.copy(inputFile = x))
      opt[String]("outputfile")
        .text(s"the path of the output csv file" +
          s"  default: ${defaultParams.outputFile}")
        .action((x, c) => c.copy(outputFile = x))
      opt[String]("host")
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("analyzer")
        .text(s"the analyzer" +
          s"  default: ${defaultParams.analyzer}")
        .action((x, c) => c.copy(analyzer = x))
      opt[String]("path")
        .text(s"the service path" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_XXX" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[Seq[String]]("item_list")
        .text(s"list of string representing the traversed states" +
          s"  default: ${defaultParams.itemList}")
        .action((x, c) => c.copy(itemList = x))
      opt[Map[String, String]]("variables")
        .text(s"set of variables to be used by the analyzers" +
          s"  default: ${defaultParams.variables}")
        .action((x, c) => c.copy(variables = x))
      opt[Int]("text1_index")
        .text(s"the index of the text1 element" +
          s"  default: ${defaultParams.text1Index}")
        .action((x, c) => c.copy(text1Index = x))
       opt[Int]("text2_index")
        .text(s"the index of the text2 element" +
          s"  default: ${defaultParams.text2Index}")
        .action((x, c) => c.copy(text2Index = x))
      opt[Int]("timeout")
        .text(s"the timeout in seconds of each insert operation" +
          s"  default: ${defaultParams.timeout}")
        .action((x, c) => c.copy(timeout = x))
      opt[Int]("skiplines")
        .text(s"skip the first N lines from vector file" +
          s"  default: ${defaultParams.skipLines}")
        .action((x, c) => c.copy(skipLines = x))
      opt[Seq[String]]("header_kv")
        .text(s"header key-value pair, as key1:value1,key2:value2" +
          s"  default: ${defaultParams.headerKv}")
        .action((x, c) => c.copy(headerKv = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        execute(params)
      case _ =>
        sys.exit(1)
    }
  }
}

