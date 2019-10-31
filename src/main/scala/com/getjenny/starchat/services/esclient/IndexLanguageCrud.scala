package com.getjenny.starchat.services.esclient

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.RefreshIndexResult
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.get.{MultiGetRequest, MultiGetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest, SearchType}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.Strings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilder

//private constructor - only the factory object can create an instance,
// in order to be sure that the index name passed as parameter is formatted correctly,
class IndexLanguageCrud private(val client: ElasticClient, val index: String) {

  private[this] val Instance = "instance"
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
      .must(QueryBuilders.matchQuery(Instance, instance))
      .must(queryBuilder)

    esCrudBase.read(finalQuery, from, sort, maxItems, searchType, aggregation, requestCache, minScore, scroll,
      scrollTime, version, fetchSource)
  }

  def create(instance: String, id: String, builder: XContentBuilder): IndexResponse = {
    require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")
    esCrudBase.create(id, builder)
  }

  def create(instance: String, id: String, fields: Map[String, String]): IndexResponse = {
    val builder: XContentBuilder = builderFromMap(instance, fields)
    create(instance, id, builder)
  }

  def scroll(scrollRequest: SearchScrollRequest): SearchResponse = esCrudBase.scroll(scrollRequest)

  def bulkCreate(elems: List[(String, XContentBuilder)]): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")
    }
    esCrudBase.bulkCreate(elems)
  }

  def update(instance: String, id: String, builder: XContentBuilder, upsert: Boolean = false): UpdateResponse = {
    require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")
    esCrudBase.update(id, builder)
  }

  def update(instance: String, id: String, fields: Map[String, String]): UpdateResponse = {
    val builder: XContentBuilder = builderFromMap(instance, fields)
    update(instance, id, builder)
  }

  def bulkUpdate(elems: List[(String, XContentBuilder)], upsert: Boolean = false): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")
    }
    esCrudBase.bulkUpdate(elems, upsert)
  }

  def delete(instance: String, queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(Instance, instance))
      .must(queryBuilder)

    esCrudBase.delete(finalQuery)
  }

  def refresh(): RefreshIndexResult = esCrudBase.refresh()

  private[this] def builderFromMap(instance: String, fields: Map[String, String]): XContentBuilder = {
    val builder = jsonBuilder().startObject()
    fields.foreach { case (k, v) => builder.field(k, v) }
    builder.field(Instance, instance)
      .endObject()
    builder
  }

  private[this] def isInstanceEvaluated(builder: XContentBuilder): Boolean = {
    val parser = builder.contentType().xContent()
      .createParser(NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, Strings.toString(builder))

    Option(if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
      ObjectPath.eval[String](Instance, parser.listOrderedMap())
    } else {
      ObjectPath.eval[String](Instance, parser.mapOrdered())
    }).nonEmpty
  }

}

object IndexLanguageCrud {
  def apply(client: ElasticClient, index: String): IndexLanguageCrud = {
    val esLanguageIndex = Index.esSystemIndexName(index, client.indexSuffix)
    new IndexLanguageCrud(client, esLanguageIndex)
  }
}
