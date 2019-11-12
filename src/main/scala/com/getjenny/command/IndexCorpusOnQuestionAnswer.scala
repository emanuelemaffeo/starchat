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
import com.getjenny.starchat.entities.{es, _}
import com.getjenny.starchat.entities.es.{Doctypes, QADocument, QADocumentAnnotations, QADocumentCore}
import com.getjenny.starchat.serializers.JsonSupport
import com.roundeights.hasher.Implicits._
import scopt.OptionParser

import scala.collection.immutable
import scala.collection.immutable.List
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.io.Source

case class IndexCorpusOnQuestionAnswerException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

object IndexCorpusOnQuestionAnswer extends JsonSupport {
  private[this] case class Params(
                                   host: String = "http://localhost:8888",
                                   indexName: String = "index_getjenny_english_0",
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

    val baseUrl = params.host + "/" + params.indexName + params.path

    val inputFile = params.inputfile match {
      case Some(path) => path
      case _ => throw IndexCorpusOnQuestionAnswerException("inputfile cannot be empty")
    }

    val lines = Source.fromFile(name=inputFile).getLines.toList

    val convItems: String => String = if (params.base64) {
      decodeBase64
    } else {
      identity
    }

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

    lines.foreach(entry => {
      val documentString = convItems(entry)
      val id: String = documentString.sha256

      val qaDocument: QADocument = QADocument(
        id = id,
        conversation = "corpora",
        indexInConversation = -1,
        coreData = Some(QADocumentCore(
          question = Some(documentString),
          answer = Some(documentString)
        )),
        annotations = Some {
          QADocumentAnnotations(
            doctype = Some(Doctypes.HIDDEN)
          )
        }
      )

      val entityFuture = Marshal(qaDocument).to[MessageEntity]
      val entity = Await.result(entityFuture, 10.second)
      val responseFuture: Future[HttpResponse] =
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = baseUrl,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.Created | StatusCodes.OK => println("indexed: " + qaDocument.id +
          " text(" + documentString + ")")
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
        .text(s"the index_name, e.g. index_english_XXX" +
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
