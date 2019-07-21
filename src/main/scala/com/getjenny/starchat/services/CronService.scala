package com.getjenny.starchat.services

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

trait CronService {
  implicit def executionContext: ExecutionContext =
    SCActorSystem.system.dispatchers.lookup("starchat.blocking-dispatcher")

  protected[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)
  protected[this] val analyzerService: AnalyzerService.type = AnalyzerService
  protected[this] val dtReloadService: DtReloadService.type = DtReloadService
  protected[this] val clusterNodesService: ClusterNodesService.type = ClusterNodesService
  protected[this] val systemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService
  protected[this] val nodeDtLoadingStatusService: NodeDtLoadingStatusService.type = NodeDtLoadingStatusService

  protected[this] val tickMessage = "tick"
}