package com.getjenny.starchat.services

import akka.actor.{Actor, Props}
import com.getjenny.starchat.services.esclient.crud.EsCrudBase
import com.getjenny.starchat.utils.Index
import org.elasticsearch.index.query.QueryBuilders

import scala.util.Try

object CronDeleteInstanceService extends CronService {

  case class DeleteInstanceResponse(indexName: String, instance: String, documentsDeleted: Long)

  class DeleteInstanceActor extends Actor {
    override def receive: Receive = {
      case `tickMessage` =>
        val instances = instanceRegistryService.getAll

        sender ! instances
          .map { case (id, doc) => id -> doc.enabled }
          .filterNot { case (_, enabled) => enabled.getOrElse(false) }
          .flatMap { case (registryEntryId, _) =>
            val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(registryEntryId, "")
            systemIndexManagementService.indices
              .filter(_.startsWith(esLanguageSpecificIndexName))
              .map(registryEntryId -> _)
          }.map { case (id, indexName) => delete(id, indexName)}


      case m => log.error("Unexpected message in  DeleteInstanceActor :{}", m)
    }

    private[this] def delete(registryEntryId: String, indexName: String): Either[String, DeleteInstanceResponse] = {
      val res = Try {
        val instance = Index.instanceName(registryEntryId)
        val crud = new EsCrudBase(systemIndexManagementService.elasticClient, indexName)
        val delete = crud.delete(QueryBuilders.matchQuery("instance", instance))
        log.info("Deleted instance: {} from index: {} - doc deleted: {}", instance, indexName, delete.getDeleted)
        instanceRegistryService.deleteEntry(List(registryEntryId))
        DeleteInstanceResponse(indexName, instance, delete.getDeleted)
      }.toEither
        .left.map{ _ =>
        log.error("Error during delete registry entry {} for in index {}", registryEntryId, indexName)
          s"Error during delete registry entry $registryEntryId for in index $indexName"
      }
      res
    }

  }

  object DeleteInstanceActor {
    def props: Props = Props(new DeleteInstanceActor)
  }

}
