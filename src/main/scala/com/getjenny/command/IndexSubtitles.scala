package com.getjenny.command

/**
  * Created by angelo on 29/03/17.
  */

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import com.getjenny.starchat.entities._
import com.getjenny.starchat.entities.es.{Agent, Answered, QADocument, QADocumentAnnotations, QADocumentCore}
import com.getjenny.starchat.serializers.JsonSupport
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.io.Source

object IndexSubtitles extends JsonSupport {

  private[this] case class Params(
                                   host: String = "http://localhost:8888",
                                   indexName: String = "index_getjenny_english_common_0",
                                   path: String = "/prior_data",
                                   inputfile: String = "subtitles.csv",
                                   separator: Char = '\t',
                                   skiplines: Int = 0,
                                   timeout: Int = 60,
                                   headerKv: Seq[String] = Seq.empty[String]
                                 )

  private[this] def execute(params: Params) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val baseUrl = params.host + "/" + params.indexName + params.path

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

    val lines = Source.fromFile(name=params.inputfile).getLines

    val docs = lines.map{ entry =>
      val fields = entry.split("\t")
      val doc = QADocument(
        id = fields(0),
        conversation = fields(1),
        indexInConversation = fields(2).toInt,
        coreData = Some {
          QADocumentCore(
            question = Some { fields(3) }
          )
        },
        annotations = Some{
          QADocumentAnnotations(
            answered = Some(Answered.UNSPECIFIED),
            agent = Some(Agent.UNSPECIFIED)
          )
        }
      )
      doc
    }

    docs.foreach(doc => {
      val entity_future = Marshal(doc).to[MessageEntity]
      val entity = Await.result(entity_future, 10.second)
      val responseFuture: Future[HttpResponse] =
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = baseUrl,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.Created | StatusCodes.OK => println("indexed: " + doc.id)
        case _ =>
          system.log.error("failed indexing document(" + doc.id + ") Message(" + result.toString() + ")")
      }
    })

    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexSubtitles") {
      head("Index data into stat_text")
      help("help").text("prints this usage text")
      opt[String]("inputfile")
        .text(s"the path of the file with the subtitles" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = x))
      opt[String]("host")
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_<language>_XXX" +
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
