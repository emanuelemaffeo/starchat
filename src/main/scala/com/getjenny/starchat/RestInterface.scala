package com.getjenny.starchat

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
  */

import akka.http.scaladsl.server.Route
import com.getjenny.starchat.resources._
import com.getjenny.starchat.services._

import scala.concurrent.ExecutionContext

trait Resources extends KnowledgeBaseResource with DecisionTableResource
  with RootAPIResource with SystemIndexManagementResource with IndexManagementResource with LanguageGuesserResource
  with TermResource with TokenizersResource with AnalyzersPlaygroundResource
  with SpellcheckResource with UserResource

trait RestInterface extends Resources {
  implicit def executionContext: ExecutionContext

  lazy val knowledgeBaseService = KnowledgeBaseService
  lazy val decisionTableService = DecisionTableService
  lazy val indexManagementService = IndexManagementService
  lazy val systemIndexManagementService = SystemIndexManagementService
  lazy val languageGuesserService = LanguageGuesserService
  lazy val termService = TermService
  lazy val responseService = ResponseService
  lazy val analyzerService = AnalyzerService
  lazy val userService = UserService
  lazy val spellcheckService = SpellcheckService
  lazy val clusterNodesServices = ClusterNodesService
  lazy val nodeDtLoadingStatusService = NodeDtLoadingStatusService
  lazy val cronReloadDTService = CronReloadDTService
  lazy val cronCleanDTService = CronCleanDTService
  lazy val cronCleanDeadNodesService = CronCleanDeadNodesService
  lazy val cronNodeAliveSignalService = CronNodeAliveSignalService
  lazy val cronCleanDtLoadingRecordsService = CronCleanDtLoadingRecordsService
  lazy val systemService = DtReloadService

  val routes: Route = rootAPIsRoutes ~
    LoggingEntities.logRequestAndResultReduced(knowledgeBaseRoutes) ~
    LoggingEntities.logRequestAndResultReduced(knowledgeBaseStreamRoutes) ~
    LoggingEntities.logRequestAndResult(knowledgeBaseSearchRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableUploadCSVRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableSearchRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableAsyncReloadRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableResponseRequestRoutes) ~
    LoggingEntities.logRequestAndResult(decisionTableAnalyzerRoutes) ~
    LoggingEntities.logRequestAndResult(postIndexManagementCreateRoutes) ~
    LoggingEntities.logRequestAndResult(postIndexManagementRefreshRoutes) ~
    LoggingEntities.logRequestAndResult(putIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResult(indexManagementRoutes) ~
    LoggingEntities.logRequestAndResult(postIndexManagementOpenCloseRoutes) ~
    LoggingEntities.logRequestAndResult(systemIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResult(systemGetIndexesRoutes) ~
    LoggingEntities.logRequestAndResult(languageGuesserRoutes) ~
    LoggingEntities.logRequestAndResultReduced(termRoutes) ~
    LoggingEntities.logRequestAndResultReduced(termStreamRoutes) ~
    LoggingEntities.logRequestAndResult(esTokenizersRoutes) ~
    LoggingEntities.logRequestAndResult(analyzersPlaygroundRoutes) ~
    LoggingEntities.logRequestAndResult(spellcheckRoutes) ~
    LoggingEntities.logRequestAndResult(postUserRoutes) ~
    LoggingEntities.logRequestAndResult(putUserRoutes) ~
    LoggingEntities.logRequestAndResult(getUserRoutes) ~
    LoggingEntities.logRequestAndResult(deleteUserRoutes) ~
    LoggingEntities.logRequestAndResult(genUserRoutes)
}
