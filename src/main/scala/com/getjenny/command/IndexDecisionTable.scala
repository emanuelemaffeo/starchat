package com.getjenny.command

/**
  * Created by angelo on 29/03/17.
  */

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import com.getjenny.starchat.serializers.JsonSupport
import com.getjenny.starchat.services.FileToDocuments
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object IndexDecisionTable extends JsonSupport {

  private[this] case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_0",
                             path: String = "/decisiontable",
                             inputfile: String = "decision_table.csv",
                             separator: Char = ',',
                             skiplines: Int = 1,
                             timeout: Int = 60,
                             headerKv: Seq[String] = Seq.empty[String]
                           )

  private[this] def execute(params: Params) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val vecsize = 0
    val skiplines = params.skiplines

    val baseUrl = params.host + "/" + params.indexName + params.path
    val file = new File(params.inputfile)

    val httpHeader: immutable.Seq[HttpHeader] = if(params.headerKv.nonEmpty) {
      val headers: Seq[RawHeader] = params.headerKv.map(x => {
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

    FileToDocuments.getDTDocumentsFromCSV(log = system.log, file = file, separator = params.separator)
      .foreach(state => {
        val entity_future = Marshal(state).to[MessageEntity]
        val entity = Await.result(entity_future, 10.second)
        val responseFuture: Future[HttpResponse] =
          Http().singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = baseUrl,
            headers = httpHeader,
            entity = entity))
        val result = Await.result(responseFuture, timeout)
        result.status match {
          case StatusCodes.Created | StatusCodes.OK => println("indexed: " + state.state)
          case _ =>
            system.log.error("failed indexing state(" + state.state + ") Message(" + result.toString() + ")")
        }
      }
    )

    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexDecisionTable") {
      head("Index data into DecisionTable")
      help("help").text("prints this usage text")
      opt[String]("inputfile")
        .text(s"the path of the file with the vectors" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = x))
      opt[String]("host")
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_XXX" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[String]("path")
        .text(s"the service path" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[Int]("timeout")
        .text(s"the timeout in seconds of each insert operation" +
          s"  default: ${defaultParams.timeout}")
        .action((x, c) => c.copy(timeout = x))
      opt[Int]("skiplines")
        .text(s"skip the first N lines from vector file" +
          s"  default: ${defaultParams.skiplines}")
        .action((x, c) => c.copy(skiplines = x))
      opt[Seq[String]]("header_kv")
        .text(s"header key-value pair, as key1:value1,key2:value2" +
          s"  default: ${defaultParams.headerKv}")
        .action((x, c) => c.copy(headerKv = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        execute(params)
        sys.exit(0)
      case _ =>
        sys.exit(1)
    }
  }
}