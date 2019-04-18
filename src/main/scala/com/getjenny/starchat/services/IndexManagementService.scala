package com.getjenny.starchat.services

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 10/03/17.
  */

import java.io._

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities._
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings._
import org.elasticsearch.common.xcontent.XContentType
import scalaz.Scalaz._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

case class IndexManagementServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class LangResourceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object IndexManagementService {
  val elasticClient: IndexManagementClient.type = IndexManagementClient
  val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  private[this] def analyzerFiles(language: String): JsonMappingAnalyzersIndexFiles =
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/" + language + "/analyzer.json",
      updatePath = "/index_management/json_index_spec/" + language + "/update/analyzer.json",
      indexSuffix = "")

  private[this] val schemaFiles: List[JsonMappingAnalyzersIndexFiles] = List[JsonMappingAnalyzersIndexFiles](
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/state.json",
      updatePath = "/index_management/json_index_spec/general/update/state.json",
      indexSuffix = elasticClient.dtIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/question.json",
      updatePath = "/index_management/json_index_spec/general/update/question.json",
      indexSuffix = elasticClient.kbIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/term.json",
      updatePath = "/index_management/json_index_spec/general/update/term.json",
      indexSuffix = elasticClient.termIndexSuffix)
  )

  private[this] object LangResourceType extends Enumeration {
    val STOPWORD,
    STEMMER_OVERRIDE,
    UNSPECIFIED = LangResourceType.Value
    def value(v: String): LangResourceType.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
  }

  private[this] val accessoryFilePathTpl = "/index_management/json_index_spec/%1$s/%2$s"
  private[this] val langSpecificDataFiles: List[(String, LangResourceType.Value)] =
    List[(String, LangResourceType.Value)](
      ("stopwords.json", LangResourceType.STOPWORD),
      ("stemmer_override.json", LangResourceType.STEMMER_OVERRIDE)
    )

  private[this] def loadLangSpecificResources(indexName: String, indexSuffix: String,
                                              language: String, openCloseIndices: Boolean = false): Unit = {
    val client: TransportClient = elasticClient.getClient()

    val resourcesJson = langSpecificDataFiles.map { case (file, resType) =>
      val resPath: String = accessoryFilePathTpl.format(language, file)
      val resFileJsonIs: Option[InputStream] = Option {
        getClass.getResourceAsStream(resPath)
      }
      resFileJsonIs match {
        case Some(stream) => (Source.fromInputStream(stream, "utf-8").mkString, resType)
        case _ => ("", resType)
      }
    }.filter{ case(json, _) => json != ""}

    val fullIndexName: String = Index.indexName(indexName, indexSuffix)
    if(openCloseIndices) openClose(indexName, Some(indexSuffix), "close")
    resourcesJson.foreach { case(resJson, resType) =>
      resType match {
        case LangResourceType.STOPWORD | LangResourceType.STEMMER_OVERRIDE =>
          val updateIndexSettingsRes =
            client.admin().indices().prepareUpdateSettings().setIndices(fullIndexName)
              .setSettings(Settings.builder().loadFromSource(resJson, XContentType.JSON)).get()
          if(!updateIndexSettingsRes.isAcknowledged) {
            val message = "Failed to apply index settings (" + resType + ") for index: " + fullIndexName
            throw LangResourceException(message)
          }
        case _ =>
          val message = "Bad ResourceType(" + resType + ") for index: " + fullIndexName
          throw LangResourceException(message)
      }
    }
    if(openCloseIndices) openClose(indexName, Some(indexSuffix), "open")
  }

  def create(indexName: String,
             indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: TransportClient = elasticClient.getClient()

    // extract language from index name
    val (language, _) = indexName match {
      case Index.indexExtractFieldsRegexDelimited(languagePattern, arbitraryPattern) =>
        (languagePattern, arbitraryPattern)
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

    val operationsMessage: List[(String, Boolean)] = schemaFiles.filter(item => {
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

      val fullIndexName = Index.indexName(indexName, item.indexSuffix)

      val createIndexRes: CreateIndexResponse =
        client.admin().indices().prepareCreate(fullIndexName)
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON))
          .setSource(schemaJson, XContentType.JSON).get()

      loadLangSpecificResources(indexName, item.indexSuffix, language, true)

      (item.indexSuffix + "(" + fullIndexName + ", " + createIndexRes.isAcknowledged + ")",
        createIndexRes.isAcknowledged)
    })

    val message = "IndexCreation: " + operationsMessage.map{case(msg, _) => msg}.mkString(" ")

    IndexManagementResponse(message = message)
  }

  def remove(indexName: String,
             indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: TransportClient = elasticClient.getClient()

    if (!elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw IndexManagementServiceException(message)
    }

    val operationsMessage: List[(String, Boolean)] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = Index.indexName(indexName, item.indexSuffix)
      val deleteIndexRes: DeleteIndexResponse =
        client.admin().indices().prepareDelete(fullIndexName).get()

      (item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged + ")",
        deleteIndexRes.isAcknowledged)
    })

    val message = "IndexDeletion: " + operationsMessage.map{case(msg, _) => msg}.mkString(" ")

    IndexManagementResponse(message = message)
  }

  def check(indexName: String,
            indexSuffix: Option[String] = None): IndexManagementResponse = {
    val client: TransportClient = elasticClient.getClient()

    val operations: List[(String, Boolean)] = schemaFiles.filter{item =>
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }.map{item =>
      val fullIndexName = Index.indexName(indexName, item.indexSuffix)

      val getMappingsReq = client.admin.indices.prepareGetMappings(fullIndexName).get()
      val check = getMappingsReq.mappings.containsKey(fullIndexName)
      (item.indexSuffix + "(" + fullIndexName + ", " + check + ")", check)
    }

    val (messages, checks) = operations.unzip
    IndexManagementResponse(message = "IndexCheck: " + messages.mkString(" "))
  }

  def openClose(indexName: String, indexSuffix: Option[String] = None,
                operation: String): List[OpenCloseIndex] = {
    val client: TransportClient = elasticClient.getClient()
    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = Index.indexName(indexName, item.indexSuffix)
      operation match {
        case "close" =>
          val closeIndexResponse: CloseIndexResponse = client.admin().indices().prepareClose(fullIndexName).get()
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = closeIndexResponse.isAcknowledged)
        case "open" =>
          val openIndexResponse: OpenIndexResponse = client.admin().indices().prepareOpen(fullIndexName).get()
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = openIndexResponse.isAcknowledged)
        case _ => throw IndexManagementServiceException("operation not supported on index: " + operation)
      }
    })
  }

  def updateSettings(indexName: String,
                     indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: TransportClient = elasticClient.getClient()

    val (language, _) = Index.patternsFromIndexName(indexName: String)

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

    val operationsMessage: List[(String, Boolean)] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = Index.indexName(indexName, item.indexSuffix)

      val updateIndexSettingsRes =
        client.admin().indices().prepareUpdateSettings().setIndices(fullIndexName)
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON)).get()

      loadLangSpecificResources(indexName, item.indexSuffix, language)

      (item.indexSuffix + "(" + fullIndexName + ", " + updateIndexSettingsRes.isAcknowledged + ")",
        updateIndexSettingsRes.isAcknowledged)
    })

    val message = "IndexSettingsUpdate: " + operationsMessage.map{case(msg, _) => msg}.mkString(" ")
    IndexManagementResponse(message = message)
  }

  def updateMappings(indexName: String,
                     indexSuffix: Option[String] = None): Future[IndexManagementResponse] = Future {
    val client: TransportClient = elasticClient.getClient()

    val operationsMessage: List[(String, Boolean)] = schemaFiles.filter(item => {
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

      val fullIndexName = Index.indexName(indexName, item.indexSuffix)

      val putMappingRes  =
        client.admin().indices().preparePutMapping().setIndices(fullIndexName)
          .setType(item.indexSuffix)
          .setSource(schemaJson, XContentType.JSON).get()

      (item.indexSuffix + "(" + fullIndexName + ", " + putMappingRes.isAcknowledged + ")",
        putMappingRes.isAcknowledged)
    })

    val message = "IndexUpdateMappings: " + operationsMessage.mkString(" ")
    IndexManagementResponse(message = message)
  }

  def refresh(indexName: String,
              indexSuffix: Option[String] = None): Future[RefreshIndexResults] = Future {
    val operationsResults: List[RefreshIndexResult] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = Index.indexName(indexName, item.indexSuffix)
      val refreshIndexRes: RefreshIndexResult = elasticClient.refreshIndex(fullIndexName)
      if (refreshIndexRes.failed_shards_n > 0) {
        val indexRefreshMessage = item.indexSuffix + "(" + fullIndexName + ", " +
          refreshIndexRes.failed_shards_n + ")"
        throw new Exception(indexRefreshMessage)
      }

      refreshIndexRes
    })

    RefreshIndexResults(results = operationsResults)
  }

}
