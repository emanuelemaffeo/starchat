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
class EsCrudBase private(client: ElasticClient, index: String) {

  private[this] val Instance = "instance"
  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  def findAll(ids: List[String]): MultiGetResponse = {
    val request = new MultiGetRequest()
    ids.foreach { id =>
      request.add(new MultiGetRequest.Item(index, id))
    }
    client.httpClient.mget(request, RequestOptions.DEFAULT)
  }

  def find(instance: String,
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

    val search: SearchSourceBuilder = new SearchSourceBuilder()
      .from(from.getOrElse(0))
      .query(finalQuery)
      .size(maxItems.getOrElse(100))

    sort.foreach(b => search.sort(b))
    aggregation.foreach(b => search.aggregation(b))
    minScore.foreach(search.minScore)
    version.foreach(search.version(_))
    fetchSource.foreach(x => search.fetchSource(x, Array.empty[String]))

    val request = new SearchRequest(index)
      .source(search)
      .searchType(searchType)

    if (scroll) request.scroll(new TimeValue(scrollTime))
    requestCache.map(request.requestCache(_))

    log.debug("Search on instance {} - search request: {}", instance, request)

    client.httpClient.search(request, RequestOptions.DEFAULT)
  }

  def scroll(scrollRequest: SearchScrollRequest): SearchResponse = {
    client.httpClient.scroll(scrollRequest, RequestOptions.DEFAULT)
  }

  def index(instance: String, id: String, builder: XContentBuilder): IndexResponse = {
    log.debug("Indexing instance {} - id: {}", instance, id)
    val request: IndexRequest = createIndexRequest(id, builder)
    client.httpClient.index(request, RequestOptions.DEFAULT)
  }

  def index(instance: String, id: String, fields: Map[String, String]): IndexResponse = {
    val builder: XContentBuilder = builderFromMap(instance, fields)
    index(instance, id, builder)
  }

  def bulkInsert(elems: List[(String, XContentBuilder)]): BulkResponse = {
    val request = new BulkRequest
    elems.foreach { case (id, builder) => request.add(createIndexRequest(id, builder))}

    client.httpClient.bulk(request, RequestOptions.DEFAULT)
  }

  def update(instance: String, id: String, builder: XContentBuilder, upsert: Boolean = false): UpdateResponse = {
    log.debug("Update instance {} - id: {}", instance, id)
    val request: UpdateRequest = createUpdateRequest(id, builder, upsert)
    client.httpClient.update(request, RequestOptions.DEFAULT)
  }

  def update(instance: String, id: String, fields: Map[String, String]): UpdateResponse = {
    val builder: XContentBuilder = builderFromMap(instance, fields)
    update(instance, id, builder)
  }

  def bulkUpdate(elems: List[(String, XContentBuilder)], upsert: Boolean = false): BulkResponse = {
    val request = new BulkRequest
    elems.foreach { case (id, builder) => request.add(createUpdateRequest(id, builder, upsert))}

    client.httpClient.bulk(request, RequestOptions.DEFAULT)
  }

  def deleteByQuery(instance: String, queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(Instance, instance))
      .must(queryBuilder)

    val request = new DeleteByQueryRequest(index)
    request.setConflicts("proceed")
    request.setQuery(finalQuery)

    log.debug("Delete instance {} - request: {}", instance, request)

    client.httpClient.deleteByQuery(request, RequestOptions.DEFAULT)
  }

  def refresh(): RefreshIndexResult = {
    client.refresh(index)
  }

  private[this] def builderFromMap(instance: String, fields: Map[String, String]): XContentBuilder = {
    val builder = jsonBuilder().startObject()
    fields.foreach { case (k, v) => builder.field(k, v) }
    builder.field(Instance, instance)
      .endObject()
    builder
  }

  private[this] def createIndexRequest(id: String, builder: XContentBuilder): IndexRequest = {
    require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")

    new IndexRequest()
      .index(index)
      .source(builder)
      .id(id)
  }

  private[this] def createUpdateRequest(id: String, builder: XContentBuilder, upsert: Boolean): UpdateRequest = {
    require(isInstanceEvaluated(builder), "instance field must be present while indexing a new document")

    new UpdateRequest()
      .index(index)
      .doc(builder)
      .id(id)
      .docAsUpsert(upsert)
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

object EsCrudBase {
  def apply(client: ElasticClient, index: String): EsCrudBase = {
    val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(index, client.indexSuffix)
    new EsCrudBase(client, esLanguageSpecificIndexName)
  }
}
