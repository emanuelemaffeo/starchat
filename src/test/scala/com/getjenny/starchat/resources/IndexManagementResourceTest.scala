package com.getjenny.starchat.resources

import akka.http.scaladsl.model.StatusCodes
import com.getjenny.starchat.entities.{CreateLanguageIndexRequest, IndexManagementResponse}

class IndexManagementResourceTest extends TestBase {

  val createEnglishRequest = CreateLanguageIndexRequest(List("english"))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Post("/language_index_management", createEnglishRequest) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
  }

  "StarChat" should {
    "return an HTTP code 200 when add instance" in {
      Post(s"/index_getjenny_english_0/index_management/create") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[IndexManagementResponse]
        response.message shouldEqual "Created instance index_getjenny_english_0, operation status: CREATED"
      }
    }

    "return an HTTP code 200 when disable instance" in {
      Post(s"/index_getjenny_english_0/index_management/disable") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[IndexManagementResponse]
        response.message shouldEqual "Disabled instance index_getjenny_english_0, operation status: OK"
      }
    }

    "return an HTTP code 200 when mark delete instance" in {
      Post(s"/index_getjenny_english_0/index_management/delete") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[IndexManagementResponse]
        response.message shouldEqual "Mark Delete instance index_getjenny_english_0, operation status: OK"
      }
    }

    "return an HTTP code 401 when trying to access to a service when instance is disabled" in {
      Get("/index_getjenny_english_0/decisiontable?id=forgot_password&id=call_operator") ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Delete("/language_index_management?index=index_english") ~>  addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
  }

}
