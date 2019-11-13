package com.getjenny.starchat.services

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.services.esclient.crud.EsCrudBase
import com.getjenny.starchat.services.esclient.{BayesOperatorCacheElasticClient, ElasticClient}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConverters._

case class BayesOperatorCacheDocument(key: String, value: Option[Double]) {
  def toBuilder: XContentBuilder = {
    jsonBuilder()
      .startObject()
      .field("value", value.getOrElse(0d))
      .endObject()
  }
}

object BayesOperatorCacheDocument {
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
  private[this] val esCrudBase = new EsCrudBase(elasticClient, indexName)

  def put(key: String, value: Double): Unit = {
    val document = BayesOperatorCacheDocument(key, Some(value))
    val response = esCrudBase.update(document.key, document.toBuilder, upsert = true)
    log.info("BayesOperatorCache put - key: {}, value: {}, operation status: {}", key, value, response.status())
  }

  def get(key: String): Option[Double] = {
    val response = esCrudBase.read(key)

    if (response.isExists) {
      BayesOperatorCacheDocument(response).value
    } else {
      None
    }
  }

  def refresh(): Unit = {
    esCrudBase.refresh()
  }

  def clear(): Unit = {
    val response = esCrudBase.delete(QueryBuilders.matchAllQuery())
    log.info("BayesOperatorCache cleared {} entries", response.getDeleted)
  }

}
