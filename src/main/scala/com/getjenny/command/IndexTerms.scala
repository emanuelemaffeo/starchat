package com.getjenny.command

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import com.getjenny.starchat.entities.{Term, Terms}
import com.getjenny.starchat.serializers.JsonSupport
import scalaz.Scalaz._
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object IndexTerms extends JsonSupport {

  private[this] case class Params(
    host: String = "http://localhost:8888",
    index_name: String = "index_0",
    path: String = "/term/index",
    inputfile: String = "vectors.txt",
    skiplines: Int = 0,
    timeout: Int = 60,
    vecsize: Int = 300,
    headerKv: Seq[String] = Seq.empty[String]
  )

  private[this] def execute(params: Params) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    
    val vecsize = params.vecsize
    val skiplines = params.skiplines

    val baseUrl = params.host + "/" + params.index_name + params.path
    lazy val termTextEntries = Source.fromFile(params.inputfile).getLines

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

    termTextEntries.drop(skiplines).foreach(entry => {
      val splitted = entry.split(" ")
      val termText = splitted.headOption.getOrElse("")
      val termVector = Try(splitted.tail.map(e => e.toDouble).toVector) match {
        case Success(tVec) => tVec
        case Failure(e) =>
          println("Error: " + e.getMessage)
          Vector.empty[Double]
      }

      if (termVector.length =/= vecsize) {
        println("Error: file row does not contains a consistent vector Row(" + entry + ")")
      } else {
        val term = Term(term = termText,
          vector = Some{termVector})

        val terms = Terms(terms = List(term))
        val entityFuture = Marshal(terms).to[MessageEntity]
        val entity = Await.result(entityFuture, 10.second)
        val responseFuture: Future[HttpResponse] =
          Http().singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = baseUrl,
            headers = httpHeader,
            entity = entity))
        val result = Await.result(responseFuture, timeout)
        result.status match {
          case StatusCodes.OK => println("indexed: " + term.term)
          case _ =>
            println("failed indexing term(" + term.term + ") Row(" + entry + ") Message(" + result.toString() + ")")
        }
      }
    })
    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexTerms") {
      head("Index vetor terms")
      help("help").text("prints this usage text")
      opt[String]("inputfile")
        .text(s"the path of the file with the vectors" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = x))
      opt[String]("host")
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("path")
        .text(s"the service path" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_XXX" +
          s"  default: ${defaultParams.index_name}")
        .action((x, c) => c.copy(index_name = x))
      opt[Int]("vecsize")
        .text(s"the vector size" +
          s"  default: ${defaultParams.vecsize}")
        .action((x, c) => c.copy(vecsize = x))
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
      case _ =>
        sys.exit(1)
    }
  }
}
