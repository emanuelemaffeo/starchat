package com.getjenny.starchat.resources

import akka.http.scaladsl.model.StatusCodes
import akka.testkit.{ImplicitSender, TestKitBase}
import com.getjenny.starchat.entities.{DTDocument, IndexDocumentResult, Permissions, User}
import com.getjenny.starchat.services.CronDeleteInstanceService.{DeleteInstanceActor, DeleteInstanceResponse}
import com.getjenny.starchat.services.esclient.{EsCrudBase, IndexManagementElasticClient}
import com.getjenny.starchat.services.{InstanceRegistryDocument, InstanceRegistryService}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CronDeleteInstancesTest extends TestEnglishBase with TestKitBase with ImplicitSender {

  private val instanceRegistryService = InstanceRegistryService
  private val indexName = "index_getjenny_english_0"
  private val client = IndexManagementElasticClient

  private val instance = Index.instanceName(indexName)
  private val deleteInstanceActor = system.actorOf(DeleteInstanceActor.props)

  "StarChat" should {

    "return an HTTP code 201 when creating a new document" in {
      val user = User(
        id = "test_user",
        password = "3c98bf19cb962ac4cd0227142b3495ab1be46534061919f792254b80c0f3e566f7819cae73bdc616af0ff555f7460ac96d88d56338d659ebd93e2be858ce1cf9",
        salt = "salt",
        permissions = Map[String, Set[Permissions.Value]]("index_getjenny_english_0" -> Set(Permissions.read, Permissions.write))
      )
      Post(s"/user", user) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
      }
      val decisionTableRequest = DTDocument(
        state = "forgot_password",
        executionOrder = 0,
        maxStateCount = 0,
        analyzer = "",
        queries = List(),
        bubble = "",
        action = "",
        actionInput = Map(),
        stateData = Map(),
        successValue = "",
        failureValue = "",
        evaluationClass = Some("default"),
        version = None
      )

      val decisionTableRequest2 = DTDocument(
        state = "dont_tell_password",
        executionOrder = 0,
        maxStateCount = 0,
        analyzer = "bor(vOneKeyword(\"password\"))",
        queries = List(),
        bubble = "Never tell your password to anyone!",
        action = "",
        actionInput = Map(),
        stateData = Map(),
        successValue = "",
        failureValue = "",
        evaluationClass = Some("default"),
        version = None
      )

      Post(s"/index_getjenny_english_0/decisiontable?refresh=1", decisionTableRequest) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
      }
      Post(s"/index_getjenny_english_0/decisiontable?refresh=1", decisionTableRequest2) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
      }
    }

    "delete instance when cron job is triggered" in {

      instanceRegistryService.markDeleteInstance(indexName)
      instanceRegistryService.esCrudBase.refresh()
      deleteInstanceActor ! "tick"

      val messages = expectMsgClass(10 seconds, classOf[List[Either[String, DeleteInstanceResponse]]])
      val registryEntry = instanceRegistryService.getInstance(indexName)

      messages.filter(_.isRight).map(_.right.get).foreach(m => {
        val indexLanguageCrud = new EsCrudBase(client, m.indexName)
        indexLanguageCrud.refresh()
        val read = indexLanguageCrud.read(QueryBuilders.matchQuery("instance", instance))
        assert(read.getHits.getHits.flatMap(_.getSourceAsMap.asScala).isEmpty)
      })

      assert(registryEntry.equals(InstanceRegistryDocument.empty))
    }
  }

}
