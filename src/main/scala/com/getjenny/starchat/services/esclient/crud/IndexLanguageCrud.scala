package com.getjenny.starchat.services.esclient.crud

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.es.{ReadEntityManager, WriteEntityManager}
import com.getjenny.starchat.entities.{DeleteDocumentResult, IndexDocumentResult, UpdateDocumentResult}
import com.getjenny.starchat.services.DeleteDataServiceException
import com.getjenny.starchat.services.esclient.ElasticClient
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.sort.SortBuilder
import scalaz.Scalaz._

import scala.collection.JavaConverters._

//private constructor - only the factory object can create an instance,
// in order to be sure that the index name and instance passed as parameters are formatted correctly,
class IndexLanguageCrud private(val client: ElasticClient, val index: String, val instance: String) {

  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  private[this] val esCrudBase = new EsCrudBase(client, index)
  private[this] val instanceFieldName = "instance"

  def readAll[T](ids: List[String], entityManager: ReadEntityManager[T]): List[T] = {
    val response = esCrudBase.readAll(ids.map(entityManager.createId(instance, _)))
    entityManager.from(response.getResponses.map(_.getResponse).filter(_.isExists).toList)
  }

  def read[T](queryBuilder: QueryBuilder,
              from: Option[Int] = None,
              sort: List[SortBuilder[_]] = List.empty,
              maxItems: Option[Int] = None,
              searchType: SearchType = SearchType.DEFAULT,
              aggregation: List[AggregationBuilder] = List.empty,
              requestCache: Option[Boolean] = None,
              minScore: Option[Float] = None,
              scroll: Boolean = false,
              scrollTime: Long = 60000,
              version: Option[Boolean] = None,
              fetchSource: Option[Array[String]] = None, entityManager: ReadEntityManager[T]): List[T] = {

    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    val response = esCrudBase.read(finalQuery, from, sort, maxItems, searchType,
      aggregation, requestCache, minScore, scroll,
      scrollTime, version, fetchSource)
    entityManager.from(response)
  }

  def scroll[T](queryBuilder: QueryBuilder,
                from: Option[Int] = None,
                sort: List[SortBuilder[_]] = List.empty,
                maxItems: Option[Int] = None,
                searchType: SearchType = SearchType.DEFAULT,
                aggregation: List[AggregationBuilder] = List.empty,
                requestCache: Option[Boolean] = None,
                minScore: Option[Float] = None,
                scrollTime: Long = 60000,
                version: Option[Boolean] = None,
                fetchSource: Option[Array[String]] = None,
                entityManager: ReadEntityManager[T]): Iterator[T] = {

    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    esCrudBase.scroll(finalQuery, from, sort, maxItems, searchType, aggregation, requestCache, minScore,
      scrollTime, version, fetchSource)
      .flatMap(x => entityManager.from(x))
  }

  def create[T](entity: T, entityManager: WriteEntityManager[T], refresh: Int = 0): IndexDocumentResult = {
    val (id, builder) = entityManager.documentBuilder(entity, instance)
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")

    val response = esCrudBase.create(id, builder)
    this.refresh(refresh)

    IndexDocumentResult(response.getIndex, entityManager.extractId(response.getId),
      response.getVersion, response.status === RestStatus.CREATED)
  }

  def bulkCreate[T](elems: List[(String, T)], entityManager: WriteEntityManager[T]): List[IndexDocumentResult] = {
    val builders = elems.map { case (_, elem) => entityManager.documentBuilder(elem, instance) }

    builders.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }
    esCrudBase.bulkCreate(builders)
      .getItems
      .map { x =>
        IndexDocumentResult(x.getIndex, entityManager.extractId(x.getId),
          x.getVersion, x.status === RestStatus.CREATED)
      }.toList
  }

  def update[T](document: T, upsert: Boolean = false, entityManager: WriteEntityManager[T], refresh: Int = 0): UpdateDocumentResult = {
    val (id, builder) = entityManager.documentBuilder(document, instance)
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")

    val readResponse = esCrudBase.read(id)
    if (readResponse.isExists && !readResponse.isSourceEmpty) {
      val savedInstance = readResponse.getSourceAsMap.asScala.getOrElse(instanceFieldName, "").toString
      if (instance =/= savedInstance) {
        log.error(s"Trying to update instance $instance with id $id owned by $savedInstance")
        throw new IllegalArgumentException(s"Trying to update instance: $instance with id: $id owned by: $savedInstance")
      }
    }

    val response = esCrudBase.update(id, builder, upsert)

    this.refresh(refresh)

    UpdateDocumentResult(response.getIndex,
      entityManager.extractId(response.getId),
      response.getVersion,
      response.status === RestStatus.CREATED)
  }

  def bulkUpdate[T](elems: List[(String, T)], upsert: Boolean = false,
                    entityManager: WriteEntityManager[T], refresh: Int = 0): List[UpdateDocumentResult] = {
    val builders = elems.map { case (_, elem) => entityManager.documentBuilder(elem, instance) }

    builders.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }

    val readResponse = esCrudBase.readAll(elems.map { case (id, _) => id })
    val otherInstancesIds = readResponse.getResponses
      .filterNot(x => x.getResponse.isSourceEmpty)
      .map(x => x.getId -> x.getResponse.getSourceAsMap.get(instanceFieldName).toString)
      .filterNot { case (_, readInstance) => readInstance === instance }
      .toSet

    if (otherInstancesIds.nonEmpty) {
      log.error("Trying to update instance {} with id owned by another instance - id list: {}", instance, otherInstancesIds.mkString(";"))
      throw new IllegalArgumentException(s"Trying to update instance: $instance with id previously created by another instance")
    }

    val response = esCrudBase.bulkUpdate(builders, upsert)

    this.refresh(refresh)

    response.getItems.map(x => UpdateDocumentResult(x.getIndex,
      entityManager.extractId(x.getId),
      x.getVersion,
      response.status === RestStatus.CREATED))
      .toList
  }

  def delete(queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    esCrudBase.delete(finalQuery)
  }

  def delete[T](ids: List[String], refresh: Int, entityManager: WriteEntityManager[T]): List[DeleteDocumentResult] = {
    val instanceIds = ids.map(entityManager.createId(instance, _))
    val response = esCrudBase.delete(instanceIds)

    this.refresh(refresh)

    response.getItems
      .map { x =>
        DeleteDocumentResult(x.getIndex, entityManager.extractId(x.getId),
          x.getVersion, x.status =/= RestStatus.NOT_FOUND)
      }.toList
  }

  def refresh(enable: Int): Unit = {
    if (enable =/= 0) {
      val refreshIndex = esCrudBase.refresh()
      if (refreshIndex.failedShardsN > 0) {
        throw DeleteDataServiceException(s"index refresh failed: ($index)")
      }
    }
  }

  private[this] def isInstanceEvaluated(builder: XContentBuilder, instance: String): Boolean = {
    val parser = builder.contentType().xContent()
      .createParser(NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, Strings.toString(builder))

    val result = Option(if (parser.nextToken() === XContentParser.Token.START_ARRAY) {
      ObjectPath.eval[String](instanceFieldName, parser.listOrderedMap())
    } else {
      ObjectPath.eval[String](instanceFieldName, parser.mapOrdered())
    })

    result.exists(_ === instance)
  }

}

object IndexLanguageCrud {
  def apply(client: ElasticClient, index: String): IndexLanguageCrud = {
    val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(index, client.indexSuffix)
    val instance = Index.instanceName(index)
    new IndexLanguageCrud(client, esLanguageSpecificIndexName, instance)
  }
}
