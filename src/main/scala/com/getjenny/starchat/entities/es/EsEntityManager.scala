package com.getjenny.starchat.entities.es

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder

import scala.util.{Failure, Success, Try}

trait EntityManager {

  protected[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  def createId(instance: String, id: String): String = {
    val instanceId = s"$instance|$id"
    if(!instanceId.matches(Index.instanceIdMatchRegex.regex)) {
      log.error(s"Invalid instance id: $id")
      throw new IllegalArgumentException(s"Invalid instance id: $instanceId")
    }
    instanceId
  }
  def extractId(id: String): String = id match {
    case Index.instanceIdMatchRegex(_, i) => i
    case _ => log.error(s"Invalid instance id: $id")
      throw new IllegalArgumentException(s"Invalid instance id: $id")
  }
}

trait EsEntityManager[I, O] extends ReadEntityManager [O] with WriteEntityManager[I]

trait ReadEntityManager[T] extends EntityManager {
  protected def fromSearchResponse(response: SearchResponse): List[T]

  protected def fromGetResponse(response: List[GetResponse]): List[T]

  def from(response: List[GetResponse]): List[T] = Try {
    fromGetResponse(response)
  } match {
    case Success(value) => value
    case Failure(e) => log.error(e, "Error while creating output document")
      throw e
  }

  def from(response: SearchResponse): List[T] = Try {
    fromSearchResponse(response)
  } match {
    case Success(value) => value
    case Failure(e) => log.error(e, "Error while creating output document: {}")
      throw e
  }
}

trait WriteEntityManager[T] extends EntityManager {
  protected def toXContentBuilder(entity: T, instance: String): (String, XContentBuilder)

  def documentBuilder(entity: T, instance: String): (String, XContentBuilder) = Try {
    toXContentBuilder(entity, instance)
  } match {
    case Success(value) => value
    case Failure(e) => log.error(e, "Error while creating XcontentBuilder")
      throw e
  }
}

