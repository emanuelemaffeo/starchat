package com.getjenny.starchat.services

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

import java.util.concurrent.ConcurrentHashMap

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.analyzer.expressions.{AnalyzersData, AnalyzersDataInternal, Context}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.analyzer.analyzers.StarChatAnalyzer
import com.getjenny.starchat.entities._
import com.getjenny.starchat.entities.es.{DecisionTableEntityManager, TextTerms}
import com.getjenny.starchat.services.esclient.DecisionTableElasticClient
import com.getjenny.starchat.services.esclient.crud.IndexLanguageCrud
import org.elasticsearch.index.query.QueryBuilders
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.collection.{concurrent, mutable}
import scala.util.{Failure, Success, Try}

case class AnalyzerServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class AnalyzerItem(declaration: String,
                        analyzer: Option[StarChatAnalyzer],
                        build: Boolean,
                        message: String)

case class DecisionTableRuntimeItem(executionOrder: Int = -1,
                                    maxStateCounter: Int = -1,
                                    analyzer: AnalyzerItem = AnalyzerItem(
                                      declaration = "", analyzer = None, build = false, message = ""
                                    ),
                                    version: Long = -1L,
                                    evaluationClass: String = "default",
                                    queries: List[String] = List.empty[String],
                                    queriesTerms: List[TextTerms] = List.empty[TextTerms]
                                   )

case class ActiveAnalyzers(
                            var analyzerMap: mutable.LinkedHashMap[String, DecisionTableRuntimeItem],
                            var lastEvaluationTimestamp: Long = 0L,
                            var lastReloadingTimestamp: Long = 0L
                          )

object AnalyzerService extends AbstractDataService {
  override val elasticClient: DecisionTableElasticClient.type = DecisionTableElasticClient

  var analyzersMap: concurrent.Map[String, ActiveAnalyzers] = new ConcurrentHashMap[String, ActiveAnalyzers]().asScala
  val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)
  private[this] val termService: TermService.type = TermService
  private[this] val decisionTableService: DecisionTableService.type = DecisionTableService
  private[this] val instanceRegistryService: InstanceRegistryService.type = InstanceRegistryService
  private[this] val nodeDtLoadingStatusService: NodeDtLoadingStatusService.type = NodeDtLoadingStatusService
  val dtMaxTables: Long = elasticClient.config.getLong("es.dt_max_tables")

  def getAnalyzers(indexName: String): mutable.LinkedHashMap[String, DecisionTableRuntimeItem] = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val query = QueryBuilders.matchAllQuery
    val decisionTableItems = indexLanguageCrud.read(query,
      maxItems = Option(10000),
      version = Option(true),
      fetchSource = Option(Array("state", "execution_order", "max_state_counter",
        "analyzer", "queries", "evaluation_class")),
      scroll = true,
      entityManager = DecisionTableEntityManager
    )

    val analyzersData: List[(String, DecisionTableRuntimeItem)] = decisionTableItems.map {
      item =>
        val queriesTerms: List[TextTerms] = item.queries.map(q => {
          val queryTerms = termService.textToVectors(indexName, q)
          queryTerms
        }).filter(_.terms.terms.nonEmpty)

        val decisionTableRuntimeItem: DecisionTableRuntimeItem =
          DecisionTableRuntimeItem(
            executionOrder = item.executionOrder,
            maxStateCounter = item.maxStateCounter,
            analyzer = AnalyzerItem(declaration = item.analyzerDeclaration, build = false,
              analyzer = None,
              message = "Analyzer index(" + indexName + ") state(" + item.state + ") not built"),
            queries = item.queries,
            queriesTerms = queriesTerms,
            evaluationClass = item.evaluationClass,
            version = item.version)
        (item.state, decisionTableRuntimeItem)
    }.sortWith {
      case ((_, decisionTableRuntimeItem1), (_, decisionTableRuntimeItem2)) =>
        decisionTableRuntimeItem1.executionOrder < decisionTableRuntimeItem2.executionOrder
    }

    //get a map of stateId -> AnalyzerItem (only if there is smt in the field "analyzer")
    mutable.LinkedHashMap(analyzersData: _*)
  }

  private[this] case class BuildAnalyzerResult(analyzer: Option[StarChatAnalyzer], version: Long,
                                               build: Boolean,
                                               message: String)

  def buildAnalyzers(indexName: String,
                     analyzersMap: mutable.LinkedHashMap[String, DecisionTableRuntimeItem],
                     incremental: Boolean = true):
  mutable.LinkedHashMap[String, DecisionTableRuntimeItem] = {
    val inPlaceIndexAnalyzers = AnalyzerService.analyzersMap.getOrElse(indexName,
      ActiveAnalyzers(mutable.LinkedHashMap.empty[String, DecisionTableRuntimeItem]))
    val result = analyzersMap.map { case (stateId, runtimeItem) =>
      val executionOrder = runtimeItem.executionOrder
      val maxStateCounter = runtimeItem.maxStateCounter
      val analyzerDeclaration = runtimeItem.analyzer.declaration
      val queries = runtimeItem.queries
      val queriesTerms = runtimeItem.queriesTerms
      val version: Long = runtimeItem.version
      val evaluationClass: String = runtimeItem.evaluationClass
      val buildAnalyzerResult: BuildAnalyzerResult =
        if (analyzerDeclaration =/= "") {
          val restrictedArgs: Map[String, String] = Map("index_name" -> indexName)
          val inPlaceAnalyzer: DecisionTableRuntimeItem =
            inPlaceIndexAnalyzers.analyzerMap.getOrElse(stateId, DecisionTableRuntimeItem())
          if (incremental && inPlaceAnalyzer.version > 0 && inPlaceAnalyzer.version === version) {
            log.debug("Analyzer already built index({}) state({}) version({}:{})",
              indexName, stateId, version, inPlaceAnalyzer.version)
            BuildAnalyzerResult(analyzer = inPlaceAnalyzer.analyzer.analyzer,
              version = version, message = "Analyzer already built: " + stateId,
              build = inPlaceAnalyzer.analyzer.build)
          } else {
            Try(new StarChatAnalyzer(analyzerDeclaration, restrictedArgs)) match {
              case Success(analyzerObject) =>
                val msg = "Analyzer successfully built index(" + indexName + ") state(" + stateId +
                  ") version(" + version + ":" + inPlaceAnalyzer.version + ")"
                log.debug(msg)
                BuildAnalyzerResult(analyzer = Some(analyzerObject),
                  version = version, message = msg, build = true)
              case Failure(e) =>
                val msg = "Error building analyzer index(" + indexName + ") state(" + stateId +
                  ") declaration(" + analyzerDeclaration +
                  ") version(" + version + ":" + inPlaceAnalyzer.version + ") : " + e.getMessage
                log.error(msg)
                BuildAnalyzerResult(analyzer = None, version = -1L, message = msg, build = false)
            }
          }
        } else {
          val msg = "index(" + indexName + ") : state(" + stateId + ") : analyzer declaration is empty"
          log.debug(msg)
          BuildAnalyzerResult(analyzer = None, version = version, message = msg, build = true)
        }

      val decisionTableRuntimeItem = DecisionTableRuntimeItem(executionOrder = executionOrder,
        maxStateCounter = maxStateCounter,
        analyzer =
          AnalyzerItem(
            declaration = analyzerDeclaration,
            build = buildAnalyzerResult.build,
            analyzer = buildAnalyzerResult.analyzer,
            message = buildAnalyzerResult.message
          ),
        version = version,
        queries = queries,
        queriesTerms = queriesTerms,
        evaluationClass = evaluationClass
      )
      (stateId, decisionTableRuntimeItem)
    }.filter { case (_, decisionTableRuntimeItem) => decisionTableRuntimeItem.analyzer.build }
    result
  }

  def loadAnalyzers(indexName: String, incremental: Boolean = true, propagate: Boolean = false): DTAnalyzerLoad = {
    val analyzerMap = buildAnalyzers(indexName = indexName,
      analyzersMap = getAnalyzers(indexName), incremental = incremental)

    val dtAnalyzerLoad = DTAnalyzerLoad(numOfEntries = analyzerMap.size)
    val activeAnalyzers: ActiveAnalyzers = ActiveAnalyzers(analyzerMap = analyzerMap,
      lastReloadingTimestamp = System.currentTimeMillis)
    if (AnalyzerService.analyzersMap.contains(indexName)) {
      AnalyzerService.analyzersMap.replace(indexName, activeAnalyzers)
    } else {
      AnalyzerService.analyzersMap.putIfAbsent(indexName, activeAnalyzers)
    }

    val nodeDtLoadingTimestamp = System.currentTimeMillis()
    if (propagate) {
      Try(instanceRegistryService.updateTimestamp(indexName, nodeDtLoadingTimestamp, refresh = 1)) match {
        case Success(dtReloadTimestamp) =>
          val ts = dtReloadTimestamp
            .getOrElse(DtReloadTimestamp(indexName, InstanceRegistryDocument.InstanceRegistryTimestampDefault))
          log.debug("setting dt reload timestamp to: {}", ts.timestamp)
          activeAnalyzers.lastReloadingTimestamp = ts.timestamp
        case Failure(e) =>
          val message = "unable to set dt reload timestamp" + e.getMessage
          log.error(message)
          throw AnalyzerServiceException(message)
      }
    }
    log.debug("updating: {} : {}", nodeDtLoadingStatusService.clusterNodesService.uuid, nodeDtLoadingTimestamp)
    nodeDtLoadingStatusService.update(dtNodeStatus =
      NodeDtLoadingStatus(index = indexName, timestamp = Some {
        nodeDtLoadingTimestamp
      }))

    dtAnalyzerLoad
  }

  def getDTAnalyzerMap(indexName: String): DTAnalyzerMap = {
    DTAnalyzerMap(AnalyzerService.analyzersMap(indexName).analyzerMap
      .map {
        case (stateName, dtRuntimeItem) =>
          val dtAnalyzer =
            DTAnalyzerItem(
              dtRuntimeItem.analyzer.declaration,
              dtRuntimeItem.analyzer.build,
              dtRuntimeItem.executionOrder,
              dtRuntimeItem.evaluationClass
            )
          (stateName, dtAnalyzer)
      }.toMap)
  }

  def evaluateAnalyzer(indexName: String, analyzerRequest: AnalyzerEvaluateRequest): Option[AnalyzerEvaluateResponse] = {
    val restrictedArgs: Map[String, String] = Map("index_name" -> indexName)

    Try(new StarChatAnalyzer(analyzerRequest.analyzer, restrictedArgs)) match {
      case Failure(exception) =>
        log.error("error during evaluation of analyzer: " + exception.getMessage)
        throw exception
      case Success(result) =>
        analyzerRequest.data match {
          case Some(data) =>
            // prepare search result for search analyzer
            val searchRes = decisionTableService.searchDtQueries(indexName, analyzerRequest)
            val analyzersInternalData = decisionTableService.resultsToMap(searchRes)
            val dataInternal = AnalyzersDataInternal(
              context = Context(indexName = indexName, stateName = analyzerRequest.stateName.getOrElse("playground")),
              traversedStates = data.traversedStates,
              extractedVariables = data.extractedVariables, data = analyzersInternalData)
            val evalRes = result.evaluate(analyzerRequest.query, dataInternal)
            val returnData = if (evalRes.data.extractedVariables.nonEmpty || evalRes.data.traversedStates.nonEmpty) {
              val dataInternal = evalRes.data
              Some(AnalyzersData(traversedStates = dataInternal.traversedStates,
                extractedVariables = dataInternal.extractedVariables))
            } else {
              Option.empty[AnalyzersData]
            }
            Some(AnalyzerEvaluateResponse(build = true,
              value = evalRes.score, data = returnData, buildMessage = "success"))

          case _ =>
            Some(AnalyzerEvaluateResponse(build = true,
              value = 0.0, data = Option.empty[AnalyzersData], buildMessage = "success"))

        }
    }
  }

}

