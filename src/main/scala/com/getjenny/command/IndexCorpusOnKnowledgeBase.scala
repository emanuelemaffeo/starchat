package com.getjenny.command

/**
  * Created by angelo on 18/04/17.
  */

import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import com.getjenny.starchat.entities._
import com.getjenny.starchat.serializers.JsonSupport
import com.roundeights.hasher.Implicits._
import scopt.OptionParser

import scala.collection.immutable
import scala.collection.immutable.List
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.io.Source

object IndexCorpusOnKnowledgeBase extends JsonSupport {
  private[this] case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_0",
                             path: String = "/knowledgebase",
                             inputfile: Option[String] = None: Option[String],
                             base64: Boolean = false,
                             separator: Char = ',',
                             skiplines: Int = 0,
                             timeout: Int = 60,
                             headerKv: Seq[String] = Seq.empty[String]
                           )

  private[this] def decodeBase64(in: String): String = {
    val decodedBytes = Base64.getDecoder.decode(in)
    val decoded = new String(decodedBytes, "UTF-8")
    decoded
  }

  private[this] def execute(params: Params) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val vecsize = 0
    val skiplines = params.skiplines

    val baseUrl = params.host + "/" + params.indexName + params.path
    val lines = Source.fromFile(name=params.inputfile.get).getLines.toList

    val convItems: String => String = if (params.base64) {
      decodeBase64
    } else {
      identity
    }

    val httpHeader: immutable.Seq[HttpHeader] = if(params.headerKv.length > 0) {
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

    lines.foreach(entry => {
      val document_string = convItems(entry)
      val id: String = document_string.sha256

      val kb_document: KBDocument = KBDocument(
        id = id,
        conversation = "corpora",
        index_in_conversation = Option { -1 },
        question = document_string,
        question_negative = None: Option[List[String]],
        question_scored_terms = None: Option[List[(String, Double)]],
        answer = document_string,
        answer_scored_terms = None: Option[List[(String, Double)]],
        topics = None: Option[String],
        dclass = None: Option[String],
        doctype = Doctypes.hidden,
        state = None: Option[String],
      )

      val entityFuture = Marshal(kb_document).to[MessageEntity]
      val entity = Await.result(entityFuture, 10.second)
      val responseFuture: Future[HttpResponse] =
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = baseUrl,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.Created | StatusCodes.OK => println("indexed: " + kb_document.id +
          " text(" + kb_document.question + ")")
        case _ =>
          println("failed indexing entry(" + entry + ") Message(" + result.toString() + ")")
      }
    })
    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexKnowledgeBase") {
      head("Index conversations into the KnowledgeBase")
      help("help").text("prints this usage text")
      opt[String]("inputfile")
        .text(s"path of the file input file, a document per line, eventually base64 encoded" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = Option(x)))
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
      opt[Int]("skiplines")
        .text(s"the number of lines to skip" +
          s"  default: ${defaultParams.skiplines}")
        .action((x, c) => c.copy(skiplines = x))
      opt[Int]("timeout")
        .text(s"the timeout in seconds of each insert operation" +
          s"  default: ${defaultParams.timeout}")
        .action((x, c) => c.copy(timeout = x))
      opt[Boolean]("base64")
        .text(s"specify if questions and answer are encoded in base 64" +
          s"  default: ${defaultParams.base64}")
        .action((x, c) => c.copy(base64 = x))
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
