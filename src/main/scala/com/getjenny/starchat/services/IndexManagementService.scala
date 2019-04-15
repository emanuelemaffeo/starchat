package com.getjenny.starchat.services

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 10/03/17.
  */

import java.io._

import com.getjenny.starchat.entities._
import com.getjenny.starchat.services.esclient.IndexManagementElasticClient
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.open.{OpenIndexRequest, OpenIndexResponse}
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.indices._
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.settings._
import org.elasticsearch.common.xcontent.XContentType
import scalaz.Scalaz._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.Future
import scala.io.Source
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.collection.{concurrent, mutable}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class IndexManagementServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object IndexManagementService extends AbstractDataService {
  override val elasticClient: IndexManagementElasticClient.type = IndexManagementElasticClient

  private[this] def analyzerFiles(language: String): JsonMappingAnalyzersIndexFiles =
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/" + language + "/analyzer.json",
      updatePath = "/index_management/json_index_spec/" + language + "/update/analyzer.json",
      indexSuffix = "")

  private[this] val schemaFiles: List[JsonMappingAnalyzersIndexFiles] = List[JsonMappingAnalyzersIndexFiles](
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/state.json",
      updatePath = "/index_management/json_index_spec/general/update/state.json",
      indexSuffix = elasticClient.dtIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/question_answer.json",
      updatePath = "/index_management/json_index_spec/general/update/question_answer.json",
      indexSuffix = elasticClient.convLogsIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/question_answer.json",
      updatePath = "/index_management/json_index_spec/general/update/question_answer.json",
      indexSuffix = elasticClient.priorDataIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/question_answer.json",
      updatePath = "/index_management/json_index_spec/general/update/question_answer.json",
      indexSuffix = elasticClient.kbIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/term.json",
      updatePath = "/index_management/json_index_spec/general/update/term.json",
      indexSuffix = elasticClient.termIndexSuffix)
  )

  val accessoryFilePathTpl = "/index_management/json_index_spec/%1$s/%2$s"
  private[this] val langSpecificDataFiles: List[String] = List[String](
    "stopwords.json"
  )

  def create(indexName: String,
             indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: RestHighLevelClient = elasticClient.httpClient

    // extract language from index name
    val (_, language, _) = indexName match {
      case Index.indexExtractFieldsRegexDelimited(orgPattern, languagePattern, arbitraryPattern) =>
        (orgPattern, languagePattern, arbitraryPattern)
      case _ => throw new Exception("index name is not well formed")
    }

    val analyzerJsonPath: String = analyzerFiles(language).path
    val analyzerJsonIs: Option[InputStream] = Option {
      getClass.getResourceAsStream(analyzerJsonPath)
    }
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "Check the file: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val jsonInStream: Option[InputStream] = Option {
        getClass.getResourceAsStream(item.path)
      }

      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.path + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = indexName + "." + item.indexSuffix

      val createIndexReq = new CreateIndexRequest(fullIndexName).settings(
        Settings.builder().loadFromSource(analyzerJson, XContentType.JSON)
          .put("index.number_of_shards", elasticClient.numberOfShards)
          .put("index.number_of_replicas", elasticClient.numberOfReplicas)
      ).source(schemaJson, XContentType.JSON)

      val createIndexRes: CreateIndexResponse = client.indices.create(createIndexReq, RequestOptions.DEFAULT)

      item.indexSuffix + "(" + fullIndexName + ", " + createIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexCreation: " + operationsMessage.mkString(" ")

    IndexManagementResponse(message)
  }

  def remove(indexName: String,
             indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: RestHighLevelClient = elasticClient.httpClient

    if (!elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw IndexManagementServiceException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix

      val deleteIndexReq = new DeleteIndexRequest(fullIndexName)

      val deleteIndexRes: AcknowledgedResponse = client.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)

      item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexDeletion: " + operationsMessage.mkString(" ")

    IndexManagementResponse(message)
  }

  def check(indexName: String,
            indexSuffix: Option[String] = None): IndexManagementResponse = {
    val client: RestHighLevelClient = elasticClient.httpClient

    val operations: List[(String, Boolean)] = schemaFiles.filter{item =>
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }.map{item =>
      val fullIndexName = indexName + "." + item.indexSuffix

      val getMappingsReq: GetMappingsRequest = new GetMappingsRequest()
        .indices(fullIndexName)

      Try(client.indices.getMapping(getMappingsReq, RequestOptions.DEFAULT)) match {
        case Success(mappingsRes) =>
          val check = mappingsRes.mappings.containsKey(fullIndexName)
          (item.indexSuffix + "(" + fullIndexName + ", " + check + ")", check)
        case Failure(exception) => (item.indexSuffix + "(" + fullIndexName + ", false)", false)
      }
    }

    val (messages, checks) = operations.unzip
    IndexManagementResponse(message = "IndexCheck: " + messages.mkString(" "),
      check = checks.fold(true){case (a, b) => a && b}
    )
  }

  def openClose(indexName: String, indexSuffix: Option[String] = None,
                operation: String): Future[List[OpenCloseIndex]] = Future {
    val client: RestHighLevelClient = elasticClient.httpClient
    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      operation match {
        case "close" =>
          val closeIndexReq = new CloseIndexRequest().indices(fullIndexName)
          val closeIndexRes: AcknowledgedResponse = client.indices.close(closeIndexReq, RequestOptions.DEFAULT)
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = closeIndexRes.isAcknowledged)
        case "open" =>
          val openIndexReq = new OpenIndexRequest().indices(fullIndexName)
          val openIndexRes: OpenIndexResponse = client.indices.open(openIndexReq, RequestOptions.DEFAULT)
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = openIndexRes.isAcknowledged)
        case _ => throw IndexManagementServiceException("operation not supported on index: " + operation)
      }
    })
  }

  def updateSettings(indexName: String,
                     indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: RestHighLevelClient = elasticClient.httpClient

    val (_, language, _) = Index.patternsFromIndexName(indexName: String)

    val analyzerJsonPath: String = analyzerFiles(language).updatePath
    val analyzerJsonIs: Option[InputStream] = Option {
      getClass.getResourceAsStream(analyzerJsonPath)
    }
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "file not found: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix

      val updateIndexSettingsReq = new UpdateSettingsRequest().indices(fullIndexName)
        .settings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON))

      val updateIndexSettingsRes: AcknowledgedResponse = client.indices
        .putSettings(updateIndexSettingsReq, RequestOptions.DEFAULT)

      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexSettingsRes.isAcknowledged.toString + ")"
    })

    val message = "IndexSettingsUpdate: " + operationsMessage.mkString(" ")

    IndexManagementResponse(message)
  }

  def updateMappings(indexName: String,
                     indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: RestHighLevelClient = elasticClient.httpClient

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val jsonInStream: Option[InputStream] = Option {
        getClass.getResourceAsStream(item.updatePath)
      }

      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.updatePath + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = indexName + "." + item.indexSuffix

      val putMappingReq = new PutMappingRequest(fullIndexName)
        .source(schemaJson, XContentType.JSON)

      val putMappingRes: AcknowledgedResponse = client.indices
        .putMapping(putMappingReq, RequestOptions.DEFAULT)

      item.indexSuffix + "(" + fullIndexName + ", " + putMappingRes.isAcknowledged.toString + ")"
    })

    val message = "IndexUpdateMappings: " + operationsMessage.mkString(" ")

    IndexManagementResponse(message)
  }

  def refresh(indexName: String,
              indexSuffix: Option[String] = None): Future[RefreshIndexResults] = Future {
    val operationsResults: List[RefreshIndexResult] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val refreshIndexRes: RefreshIndexResult = elasticClient.refresh(fullIndexName)
      if (refreshIndexRes.failedShardsN > 0) {
        val indexRefreshMessage = item.indexSuffix + "(" + fullIndexName + ", " +
          refreshIndexRes.failedShardsN + ")"
        throw new Exception(indexRefreshMessage)
      }

      refreshIndexRes
    })

    RefreshIndexResults(results = operationsResults)
  }

}


