package com.getjenny.starchat.services

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 23/01/19.
 */

import akka.actor.{Actor, Props}
import com.getjenny.starchat.SCActorSystem

import scala.concurrent.duration._
import scala.language.postfixOps

/** Clean dead nodes from the cluster's node table (clean on Elasticsearch).
 */
object CronCleanDtLoadingRecordsService extends CronService {
  class CleanDtLoadingStatusTickActor extends Actor {
    def receive: PartialFunction[Any, Unit] = {
      case `tickMessage` =>
        if(nodeDtLoadingStatusService.elasticClient.existsIndices(List(nodeDtLoadingStatusService.indexName))) {
          val cleanedNodes = nodeDtLoadingStatusService.cleanDeadNodesRecords
          if (cleanedNodes.deleted > 0) {
            log.debug("Cleaned {} nodes: {}", cleanedNodes.deleted, cleanedNodes.message)
          }
        } else log.debug("index does not exists: {}", nodeDtLoadingStatusService.indexName)
      case _ =>
        log.error("Unknown error cleaning cluster nodes tables")
    }
  }

  def scheduleAction(): Unit = {
    val actorRef =
      SCActorSystem.system.actorOf(Props(new CleanDtLoadingStatusTickActor))
    SCActorSystem.system.scheduler.schedule(
      0 seconds,
      systemIndexManagementService.elasticClient.clusterCleanDtLoadingRecordsInterval seconds,
      actorRef,
      tickMessage)
  }

}
