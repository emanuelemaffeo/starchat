package com.getjenny.command

/**
  * Created by angelo on 11/04/17.
  */

import akka.http.scaladsl.model.HttpRequest
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.unmarshalling.Unmarshal

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import com.getjenny.starchat.entities._
import com.getjenny.starchat.serializers.JsonSupport
import scopt.OptionParser
import breeze.io.CSVReader
import au.com.bytecode.opencsv.CSVWriter

import scala.concurrent.Await
import scala.collection.immutable
import scala.collection.immutable.{List, Map}
import java.io.{File, FileReader, FileWriter}
import com.getjenny.analyzer.expressions.Data

object SimilarityTest extends JsonSupport {

  private case class Params(
                            host: String = "http://localhost:8888",
                            index_name: String = "index_0",
                            path: String = "/analyzers_playground",
                            inputfile: String = "pairs.csv",
                            outputfile: String = "output.csv",
                            analyzer: String = "keyword(\"test\")",
                            item_list: Seq[String] = Seq.empty[String],
                            variables: Map[String, String] = Map.empty[String, String],
                            text1_index: Int = 3,
                            text2_index: Int = 4,
                            separator: Char = ',',
                            skiplines: Int = 1,
                            timeout: Int = 60,
                            header_kv: Seq[String] = Seq.empty[String]
                           )

  private def doCalcAnalyzer(params: Params) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val vecsize = 0
    val skiplines = params.skiplines

    val base_url = params.host + "/" + params.index_name + params.path
    val file = new File(params.inputfile)
    val file_reader = new FileReader(file)
    lazy val term_text_entries = CSVReader.read(input=file_reader, separator=params.separator,
      quote = '"', skipLines=skiplines)

    val httpHeader: immutable.Seq[HttpHeader] = if(params.header_kv.length > 0) {
      val headers: Seq[RawHeader] = params.header_kv.map(x => {
        val header_opt = x.split(":")
        val key = header_opt(0)
        val value = header_opt(1)
        RawHeader(key, value)
      }) ++ Seq(RawHeader("application", "json"))
      headers.to[immutable.Seq]
    } else {
      immutable.Seq(RawHeader("application", "json"))
    }

    val timeout = Duration(params.timeout, "s")

    val out_file = new File(params.outputfile)
    val file_writer = new FileWriter(out_file)
    val output_csv = new CSVWriter(file_writer, params.separator, '"')

    term_text_entries.foreach(entry => {

      val text1 = entry(params.text1_index).toString
      val text2 = entry(params.text2_index).toString
      val escaped_text1 = text1.replace("\"", "\\\"")
      val escaped_text2 = text2.replace("\"", "\\\"")

      val analyzer: String =
        params.analyzer.replace("%text1", escaped_text1).replace("%text2", escaped_text2)
      val evaluate_request = AnalyzerEvaluateRequest(
        analyzer = analyzer,
        query = text2,
        data = Option{ Data(extracted_variables = params.variables, item_list = params.item_list.toList) }
      )

      val entity_future = Marshal(evaluate_request).to[MessageEntity]
      val entity = Await.result(entity_future, 10.second)
      val responseFuture: Future[HttpResponse] =
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = base_url,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.OK => {
          val response =
            Unmarshal(result.entity).to[AnalyzerEvaluateResponse]
          val value = response.value.get.get
          val score = value.value.toString
          val input_csv_fields = entry.toArray
          val csv_line = input_csv_fields ++ Array(score)
          output_csv.writeNext(csv_line)
        }
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
      opt[String]("inputfile").optional()
        .text(s"the path of the csv file with sentences" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = x))
      opt[String]("outputfile").optional()
        .text(s"the path of the output csv file" +
          s"  default: ${defaultParams.outputfile}")
        .action((x, c) => c.copy(outputfile = x))
      opt[String]("host").optional()
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("analyzer").optional()
        .text(s"the analyzer" +
          s"  default: ${defaultParams.analyzer}")
        .action((x, c) => c.copy(analyzer = x))
      opt[String]("path").optional()
        .text(s"the service path" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_XXX" +
          s"  default: ${defaultParams.index_name}")
        .action((x, c) => c.copy(index_name = x))
      opt[Seq[String]]("item_list").optional()
        .text(s"list of string representing the traversed states" +
          s"  default: ${defaultParams.item_list}")
        .action((x, c) => c.copy(item_list = x))
      opt[Map[String, String]]("variables").optional()
        .text(s"set of variables to be used by the analyzers" +
          s"  default: ${defaultParams.variables}")
        .action((x, c) => c.copy(variables = x))
      opt[Int]("text1_index").optional()
        .text(s"the index of the text1 element" +
          s"  default: ${defaultParams.text1_index}")
        .action((x, c) => c.copy(text1_index = x))
       opt[Int]("text2_index").optional()
        .text(s"the index of the text2 element" +
          s"  default: ${defaultParams.text2_index}")
        .action((x, c) => c.copy(text2_index = x))
      opt[Int]("timeout").optional()
        .text(s"the timeout in seconds of each insert operation" +
          s"  default: ${defaultParams.timeout}")
        .action((x, c) => c.copy(timeout = x))
      opt[Int]("skiplines").optional()
        .text(s"skip the first N lines from vector file" +
          s"  default: ${defaultParams.skiplines}")
        .action((x, c) => c.copy(skiplines = x))
      opt[Seq[String]]("header_kv").optional()
        .text(s"header key-value pair, as key1:value1,key2:value2" +
          s"  default: ${defaultParams.header_kv}")
        .action((x, c) => c.copy(header_kv = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doCalcAnalyzer(params)
      case _ =>
        sys.exit(1)
    }
  }
}