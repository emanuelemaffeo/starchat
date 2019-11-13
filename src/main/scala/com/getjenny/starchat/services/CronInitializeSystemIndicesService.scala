package com.getjenny.starchat.services

import akka.actor.{Actor, Props}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.services.esclient.SystemIndexManagementElasticClient

import scala.concurrent.duration._
import scala.language.postfixOps

/** Initialize the System Indices if they does not exists
 */
object CronInitializeSystemIndicesService extends CronService {

  class InitializeSystemIndicesActor extends Actor {
    val client: SystemIndexManagementElasticClient.type = systemIndexManagementService.elasticClient
    val indices = List(
      client.indexName + "." + client.userIndexSuffix,
      client.indexName + "." + client.systemInstanceRegistrySuffix,
      client.indexName + "." + client.systemClusterNodesIndexSuffix,
      client.indexName + "." + client.systemDtNodesStatusIndexSuffix
    )

    def receive: PartialFunction[Any, Unit] = {
      case `tickMessage` =>
        if(client.existsIndices(indices))
          log.debug("System indices exist")
        else {
          log.info("System indices are missing, initializing system indices")
          systemIndexManagementService.create()
        }
      case _ =>
        log.error("Unknown error initializing system indices.")
    }
  }

  def scheduleAction(): Unit = {
    val actorRef =
      SCActorSystem.system.actorOf(Props(new InitializeSystemIndicesActor))
    SCActorSystem.system.scheduler.scheduleOnce(
      0 seconds,
      actorRef,
      tickMessage)
  }
}
