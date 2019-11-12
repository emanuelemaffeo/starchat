package com.getjenny.starchat.services.esclient

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.RefreshIndexResult
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.get.{GetRequest, GetResponse, MultiGetRequest, MultiGetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest, SearchType}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilder

class EsCrudBase(val client: ElasticClient, val index: String) {

  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  def read(id: String): GetResponse = {
    val request = new GetRequest()
      .index(index)
      .id(id)

    client.httpClient.get(request, RequestOptions.DEFAULT)
  }

  def read(queryBuilder: QueryBuilder,
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

    val search: SearchSourceBuilder = new SearchSourceBuilder()
      .from(from.getOrElse(0))
      .query(queryBuilder)
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

    log.debug("Search request: {}", request)

    client.httpClient.search(request, RequestOptions.DEFAULT)
  }

  def readAll(ids: List[String]): MultiGetResponse = {
    val request = new MultiGetRequest()
    ids.foreach { id =>
      request.add(new MultiGetRequest.Item(index, id))
    }
    client.httpClient.mget(request, RequestOptions.DEFAULT)
  }

  def create(id: String, builder: XContentBuilder): IndexResponse = {
    log.debug("Indexing id: {}", id)
    val request: IndexRequest = createIndexRequest(id, builder)
    client.httpClient.index(request, RequestOptions.DEFAULT)
  }

  def bulkCreate(elems: List[(String, XContentBuilder)]): BulkResponse = {
    val request = new BulkRequest
    elems.foreach { case (id, builder) =>
      request.add(createIndexRequest(id, builder))
    }
    bulkUpdate(request)
  }

  def update(id: String, builder: XContentBuilder, upsert: Boolean = false): UpdateResponse = {
    log.debug("Update id: {}", id)
    val request: UpdateRequest = createUpdateRequest(id, builder, upsert)
    client.httpClient.update(request, RequestOptions.DEFAULT)
  }

  def bulkUpdate(elems: List[(String, XContentBuilder)], upsert: Boolean = false): BulkResponse = {
    val request = new BulkRequest
    elems.foreach { case (id, builder) =>
      request.add(createUpdateRequest(id, builder, upsert))
    }
    bulkUpdate(request)
  }

  private[this] def createIndexRequest(id: String, builder: XContentBuilder): IndexRequest = {
    new IndexRequest()
      .index(index)
      .source(builder)
      .create(true)
      .id(id)
  }

  private[this] def createUpdateRequest(id: String, builder: XContentBuilder, upsert: Boolean): UpdateRequest = {
    new UpdateRequest()
      .index(index)
      .doc(builder)
      .id(id)
      .docAsUpsert(upsert)
  }

  def delete(queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val request = new DeleteByQueryRequest(index)
    request.setConflicts("proceed")
    request.setQuery(queryBuilder)

    log.debug("Delete request: {}", request)

    client.httpClient.deleteByQuery(request, RequestOptions.DEFAULT)
  }

  def refresh(): RefreshIndexResult = {
    client.refresh(index)
  }

  def scroll(scrollRequest: SearchScrollRequest): SearchResponse = {
    client.httpClient.scroll(scrollRequest, RequestOptions.DEFAULT)
  }

  private[this] def bulkUpdate(request: BulkRequest): BulkResponse = {
    client.httpClient.bulk(request, RequestOptions.DEFAULT)
  }

}
