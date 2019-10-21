package com.getjenny.starchat.utils

import com.getjenny.starchat.services.DtReloadService.elasticClient
import com.getjenny.starchat.services.TermService.elasticClient
import com.getjenny.starchat.services.esclient.ElasticClient
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchType}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryRequest}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

class EsCrudBase(client: ElasticClient, index: String) {

  val completeIndexName = Index.indexName(index, client.indexSuffix)

  def find(instance: String,
           query: BoolQueryBuilder,
           sortBy: String,
           sortOrder: SortOrder = SortOrder.ASC,
           maxItems: Option[Long] = None,
           searchType: SearchType = SearchType.DEFAULT): SearchResponse = {

    val finalQuery = query.filter(QueryBuilders.termsQuery("instance", instance))

    val sourceReq: SearchSourceBuilder = new SearchSourceBuilder()
      .query(finalQuery)
      .size(maxItems.getOrElse(100L).toInt)
      .version(true)
      .sort(sortBy, sortOrder)

    val searchReq = new SearchRequest(completeIndexName)
      .source(sourceReq)
      .scroll(new TimeValue(60000))
      .searchType(searchType)

    client.httpClient.search(searchReq, RequestOptions.DEFAULT)
  }

  def update(instance: String, id: String, fields: Map[String, String]): UpdateResponse = {
    val builder = jsonBuilder().startObject()
    fields.foreach { case (k, v) => builder.field(k, v) }
    builder.field("instance", instance)
    .endObject()


    import org.elasticsearch.common.Strings
    println(s"Insert: ${Strings.toString(builder)}")

    val updateReq = new UpdateRequest()
      .index(completeIndexName)
      .doc(builder)
      .id(id)
      .docAsUpsert(true)

    client.httpClient.update(updateReq, RequestOptions.DEFAULT)
  }

  def deleteByQuery(instance: String, query: BoolQueryBuilder): BulkByScrollResponse = {
    val finalQuery = query.filter(QueryBuilders.termsQuery("instance", instance))

    val request: DeleteByQueryRequest =
      new DeleteByQueryRequest(completeIndexName)
    request.setConflicts("proceed")
    request.setQuery(finalQuery)

    client.httpClient.deleteByQuery(request, RequestOptions.DEFAULT)
  }

}
