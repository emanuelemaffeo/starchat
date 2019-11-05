package com.getjenny.starchat.services

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 23/08/17.
 */

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.{DeleteDocumentsResult, DtReloadTimestamp, IndexManagementResponse}
import com.getjenny.starchat.services.esclient.{EsCrudBase, SystemIndexManagementElasticClient}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Map

case class InstanceRegistryDocument(timestamp: Option[Long] = None, enabled: Option[Boolean] = None,
                                    delete: Option[Boolean] = None) {
  def builder: XContentBuilder = {
    val builder = jsonBuilder().startObject()
    timestamp.foreach(t => builder.field("timestamp", t))
    enabled.foreach(e => builder.field("enabled", e))
    delete.foreach(d => builder.field("delete", d))
    builder.endObject()
  }
}

object InstanceRegistryDocument {
  val InstanceRegistryTimestampDefault: Long = 0

  def apply(source: Map[String, Any]): InstanceRegistryDocument = {
    val timestamp = source.get("timestamp") match {
      case Some(value: Long) => value
      case Some(value: Int) => value.toLong
      case _ => InstanceRegistryTimestampDefault
    }
    val enabled = source.get("enabled").map(_.asInstanceOf[Boolean])
    val delete = source.get("delete").map(_.asInstanceOf[Boolean])
    InstanceRegistryDocument(Option(timestamp), enabled, delete)
  }

  def empty: InstanceRegistryDocument = {
    InstanceRegistryDocument()
  }
}

object InstanceRegistryService extends AbstractDataService {
  override val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)
  private[this] val InstanceRegistryIndex: String = Index.indexName(elasticClient.indexName,
    elasticClient.systemInstanceRegistrySuffix)
  val esCrudBase = new EsCrudBase(elasticClient, InstanceRegistryIndex)

  val cache = new TrieMap[String, InstanceRegistryDocument]()

  def updateTimestamp(dtIndexName: String, timestamp: Long = InstanceRegistryDocument.InstanceRegistryTimestampDefault,
                      refresh: Int = 0): Option[DtReloadTimestamp] = {
    val ts: Long = if (timestamp === InstanceRegistryDocument.InstanceRegistryTimestampDefault) System.currentTimeMillis else timestamp

    val response = updateInstance(dtIndexName, timestamp = Option(ts), enabled = None, delete = None)

    log.debug("dt reload timestamp response status: {}", response.status())

    if (refresh =/= 0) {
      val refreshIndex = esCrudBase.refresh()
      if (refreshIndex.failedShardsN > 0) {
        throw new Exception("System: index refresh failed: (" + InstanceRegistryIndex + ", " + dtIndexName + ")")
      }
    }
    Option {
      DtReloadTimestamp(indexName = InstanceRegistryIndex, timestamp = ts)
    }
  }

  def addInstance(indexName: String): IndexManagementResponse = {
    val document = InstanceRegistryDocument(Option(InstanceRegistryDocument.InstanceRegistryTimestampDefault),
      Option(true))
    val response = esCrudBase.create(indexName, document.builder)
    cache.put(indexName, document)
    IndexManagementResponse(s"Created instance $indexName, operation status: ${response.status}", check = true)
  }

  def getInstance(indexName: String): InstanceRegistryDocument = {
    cache.getOrElseUpdate(indexName, findEsLanguageIndex(indexName))
  }

  def disableInstance(indexName: String): IndexManagementResponse = {
    val response = updateInstance(indexName, timestamp = None, enabled = Option(false), delete = None)
    IndexManagementResponse(s"Disabled instance $indexName, operation status: ${response.status}", check = true)
  }

  def markDeleteInstance(indexName: String): IndexManagementResponse = {
    val response = updateInstance(indexName, timestamp = None, enabled = Option(false), delete = Option(true))
    IndexManagementResponse(s"Mark Delete instance $indexName, operation status: ${response.status}", check = true)
  }

  private[this] def updateInstance(indexName: String, timestamp: Option[Long],
                                   enabled: Option[Boolean], delete: Option[Boolean]): UpdateResponse = {

    val toBeUpdated = InstanceRegistryDocument(timestamp =  timestamp , enabled = enabled, delete = delete)
    val response = esCrudBase.update(indexName, toBeUpdated.builder)
    esCrudBase.refresh()
    log.debug("Updated instance {} with {} on index {}", indexName, enabled, indexName)

    val updatedDocument = findEsLanguageIndex(indexName)
    cache.put(indexName, updatedDocument)
    response
  }

  private[this] def findEsLanguageIndex(dtIndexName: String): InstanceRegistryDocument = {
    val response = esCrudBase.read(dtIndexName)
    if (!response.isExists || response.isSourceEmpty) {
      InstanceRegistryDocument.empty
    } else {
      InstanceRegistryDocument(response.getSource.asScala.toMap)
    }
  }

  def instanceTimestamp(dtIndexName: String): DtReloadTimestamp = {
    val document = getInstance(dtIndexName)

    DtReloadTimestamp(indexName = dtIndexName, timestamp = document.timestamp
      .getOrElse(InstanceRegistryDocument.InstanceRegistryTimestampDefault))
  }

  def deleteEntry(ids: List[String]): DeleteDocumentsResult = {
    val result = delete(indexName = InstanceRegistryIndex, ids = ids, refresh = 1)
    ids.foreach(cache.remove)
    result
  }

  def getAll: List[(String, InstanceRegistryDocument)] = {
    val response = esCrudBase.read(QueryBuilders.matchAllQuery())

    response.getHits.getHits.map { x =>
      x.getId -> InstanceRegistryDocument(x.getSourceAsMap.asScala.toMap)
    }.toList
  }

  def allInstanceTimestamp(minTimestamp: Option[Long] = None,
                           maxItems: Option[Long] = None): List[DtReloadTimestamp] = {
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    minTimestamp match {
      case Some(minTs) => boolQueryBuilder.filter(
        QueryBuilders.rangeQuery(elasticClient.dtReloadTimestampFieldName).gt(minTs))
      case _ => ;
    }

    val scrollResp = esCrudBase.read(boolQueryBuilder,
      maxItems = maxItems.orElse(Option(100L)).map(_.toInt),
      version = Option(true),
      sort = List(new FieldSortBuilder(elasticClient.dtReloadTimestampFieldName).order(SortOrder.DESC)),
      scroll = true)

    val dtReloadTimestamps: List[DtReloadTimestamp] = scrollResp.getHits.getHits.toList
      .map { timestampEntry =>
        val item: SearchHit = timestampEntry
        val docId: String = item.getId // the id is the index name

        val document = InstanceRegistryDocument(item.getSourceAsMap.asScala.toMap)
        DtReloadTimestamp(docId, document.timestamp
          .getOrElse(InstanceRegistryDocument.InstanceRegistryTimestampDefault))
      }
    dtReloadTimestamps
  }

}
