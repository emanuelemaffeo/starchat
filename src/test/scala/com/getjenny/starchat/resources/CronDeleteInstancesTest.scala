package com.getjenny.starchat.resources

import akka.http.scaladsl.model.StatusCodes
import akka.testkit.{ImplicitSender, TestKitBase}
import com.getjenny.starchat.entities.{Term, Terms, UpdateDocumentsResult}
import com.getjenny.starchat.services.CronDeleteInstanceService.{DeleteInstanceActor, DeleteInstanceResponse}
import com.getjenny.starchat.services.esclient.{EsCrudBase, IndexManagementElasticClient}
import com.getjenny.starchat.services.{InstanceRegistryDocument, InstanceRegistryService}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class CronDeleteInstancesTest extends TestEnglishBase with TestKitBase with ImplicitSender {

  private val instanceRegistryService = InstanceRegistryService
  private val indexName = "index_getjenny_english_0"
  private val client = IndexManagementElasticClient

  private val instance = Index.instanceName(indexName)
  private val deleteInstanceActor = system.actorOf(DeleteInstanceActor.props)

  "StarChat" should {

    "return an HTTP code 201 when creating a new document" in {
      val terms = Terms(
        terms = List(Term(term = "term1"), Term(term = "term2"))
      )
      Post("/index_getjenny_english_0/term/index?refresh=1", terms) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[UpdateDocumentsResult]
        response.data.length should be(terms.terms.length)
      }
    }

    "delete instance when cron job is triggered" in {

      val crud = new EsCrudBase(client, "index_english.term")
      crud.refresh()

      instanceRegistryService.markDeleteInstance(indexName)

      deleteInstanceActor ! "tick"

      val messages = expectMsgClass(10 seconds, classOf[List[Either[String, DeleteInstanceResponse]]])

      val registryEntry = instanceRegistryService.getInstance(indexName)
      assert(!messages.exists(_.isLeft))

      messages.filter(_.isRight).map(_.right.get).foreach(m => {
        val indexLanguageCrud = new EsCrudBase(client, m.indexName)
        indexLanguageCrud.refresh()
        val read = indexLanguageCrud.read(QueryBuilders.matchQuery("instance", instance))
        assert(read.getHits.getHits.flatMap(_.getSourceAsMap.asScala).isEmpty)
      })

      assert(registryEntry === InstanceRegistryDocument.empty)
    }
  }

}
