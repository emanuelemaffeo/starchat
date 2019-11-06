package com.getjenny.starchat.services
import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.DeleteDocumentsSummaryResult
import com.getjenny.starchat.services.esclient.{BayesOperatorCacheElasticClient, ElasticClient}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._

import scala.collection.JavaConverters._

case class BayesOperatorCacheDocument(key: String, value: Option[Double]) {
  def toBuilder: XContentBuilder = {
    jsonBuilder()
      .startObject()
      .field("value", value.getOrElse(0d))
      .endObject()
  }
}

object BayesOperatorCacheDocument{
  def apply(response: GetResponse): BayesOperatorCacheDocument = {
    new BayesOperatorCacheDocument(
      response.getId,
      response.getSourceAsMap.asScala.get("value").map(_.asInstanceOf[Double])
    )
  }
}

object BayesOperatorCacheService extends AbstractDataService {
  override protected[this] val elasticClient: ElasticClient = BayesOperatorCacheElasticClient
  private[this] val indexName = Index.indexName(elasticClient.indexName, elasticClient.indexSuffix)
  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  def put(key: String, value: Double): Unit = {
    val document = BayesOperatorCacheDocument(key, Some(value))
    val request = new IndexRequest()
      .index(indexName)
      .source(document.toBuilder)
      .id(document.key)
    
    val response = elasticClient.httpClient.index(request, RequestOptions.DEFAULT)
    log.info("BayesOperatorCache put - key: {}, value: {}, operation status: {}", key, value, response.status())
  }

  def get(key: String): Option[Double] = {
    val request = new GetRequest()
      .index(indexName)
      .id(key)

    val response = elasticClient.httpClient.get(request, RequestOptions.DEFAULT)

    if(response.isExists){
      BayesOperatorCacheDocument(response).value
    } else {
      None
    }
  }
}
