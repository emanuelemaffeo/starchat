import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit._
import com.getjenny.starchat.StarChatService
import com.getjenny.starchat.entities._
import com.getjenny.starchat.serializers.JsonSupport
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class DecisionTableResourceTest extends WordSpec with Matchers with ScalatestRouteTest with JsonSupport {
  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(10.seconds.dilated(system))

  val service = new StarChatService
  val routes = service.routes

  val testAdminCredentials = BasicHttpCredentials("admin", "adminp4ssw0rd")
  val testUserCredentials = BasicHttpCredentials("test_user", "p4ssw0rd")

  "StarChat" should {
    "return an HTTP code 200 when creating a new system index" in {
      Post(s"/system_index_management/create") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val index_name_regex = "(?:[A-Za-z0-9_]+)"
        val response = responseAs[IndexManagementResponse]
        response.message should fullyMatch regex "IndexCreation: " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\) " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\) " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\) " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\)".r
      }
    }
  }

  it should {
    "return an HTTP code 200 when creating a new user" in {
      val user = User(
        id = "test_user",
        password = "3c98bf19cb962ac4cd0227142b3495ab1be46534061919f792254b80c0f3e566f7819cae73bdc616af0ff555f7460ac96d88d56338d659ebd93e2be858ce1cf9",
        salt = "salt",
        permissions = Map[String, Set[Permissions.Value]]("index_english_0" -> Set(Permissions.read, Permissions.write))
      )
      Post(s"/user", user) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should {
    "return an HTTP code 200 when creating a new index" in {
      Post(s"/index_english_0/index_management/create") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val index_name_regex = "index_(?:[a-z]+)_(?:[A-Za-z0-9_]+)"
        val response = responseAs[IndexManagementResponse]
        response.message should fullyMatch regex "IndexCreation: " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\) " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\) " +
          "\\(" + index_name_regex + "\\.(?:[A-Za-z0-9_]+), true\\)".r
      }
    }
  }

  it should {
    "return an HTTP code 200 when indexing a decision table from csv file" in {
      val input_file = getClass.getResourceAsStream("/doc/decision_table_starchat_doc.csv")
      val input_data = scala.io.Source.fromInputStream(input_file).mkString

      val multipartForm =
        Multipart.FormData(
          Multipart.FormData.BodyPart.Strict(
            "csv",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, input_data),
            Map("filename" -> "data.csv")))

      Post(s"/index_english_0/decisiontable_upload_csv", multipartForm) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should {
    "return an HTTP code 200 when deleting an index" in {
      Delete(s"/index_english_0/index_management") ~>  addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[IndexManagementResponse]
      }
    }
  }

  it should {
    "return an HTTP code 200 when deleting an existing system index" in {
      Delete(s"/system_index_management") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[IndexManagementResponse]
      }
    }
  }

}

