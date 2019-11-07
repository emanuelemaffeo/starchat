package com.getjenny.starchat.services.esclient

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.RefreshIndexResult
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.{SearchResponse, SearchScrollRequest, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.sort.SortBuilder
import scala.collection.JavaConverters._
import scalaz.Scalaz._

//private constructor - only the factory object can create an instance,
// in order to be sure that the index name passed as parameter is formatted correctly,
class IndexLanguageCrud private(val client: ElasticClient, val index: String) {

  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)


  private[this] val instanceFieldName = "instance"
  private[this] val esCrudBase = new EsCrudBase(client, index)

  def readAll(ids: List[String]): MultiGetResponse = esCrudBase.readAll(ids)

  def read(instance: String,
           queryBuilder: QueryBuilder,
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
           fetchSource: Option[Array[String]] = None): SearchResponse = {

    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    esCrudBase.read(finalQuery, from, sort, maxItems, searchType, aggregation, requestCache, minScore, scroll,
      scrollTime, version, fetchSource)
  }

  def create(instance: String, id: String, builder: XContentBuilder): IndexResponse = {
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    esCrudBase.create(id, builder)
  }

  def scroll(scrollRequest: SearchScrollRequest): SearchResponse = esCrudBase.scroll(scrollRequest)

  def bulkCreate(instance: String, elems: List[(String, XContentBuilder)]): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }
    esCrudBase.bulkCreate(elems)
  }

  def update(instance: String, id: String, builder: XContentBuilder, upsert: Boolean = false): UpdateResponse = {
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")

    val readResponse = esCrudBase.read(id)
    if (readResponse.isExists && !readResponse.isSourceEmpty) {
      val savedInstance = readResponse.getSourceAsMap.asScala.getOrElse(instanceFieldName, "").toString
      if(instance =/= savedInstance) {
        log.error(s"Trying to update instance $instance with id $id owned by $savedInstance")
        throw new IllegalArgumentException(s"Trying to update instance: $instance with id: $id owned by: $savedInstance")
      }
    }

    esCrudBase.update(id, builder, upsert)
  }

  def bulkUpdate(instance: String, elems: List[(String, XContentBuilder)], upsert: Boolean = false): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }

    val readResponse = readAll(elems.map { case (id, _) => id })
    val otherInstancesIds = readResponse.getResponses
      .filterNot(x => x.getResponse.isSourceEmpty)
      .map(x => x.getId -> x.getResponse.getSourceAsMap.get(instanceFieldName).toString)
      .filterNot { case (_, readInstance) => readInstance === instance }
      .toSet

    if (otherInstancesIds.nonEmpty){
      log.error("Trying to update instance {} with id owned by another instance - id list: {}", instance, otherInstancesIds.mkString(";"))
      throw new IllegalArgumentException(s"Trying to update instance: $instance with id previously created by another instance")
    }

    esCrudBase.bulkUpdate(elems, upsert)
  }

  def delete(instance: String, queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    esCrudBase.delete(finalQuery)
  }

  def refresh(): RefreshIndexResult = esCrudBase.refresh()

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
    new IndexLanguageCrud(client, esLanguageSpecificIndexName)
  }
}
