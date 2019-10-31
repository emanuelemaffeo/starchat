package com.getjenny.starchat.resources

import akka.http.scaladsl.model.StatusCodes
import com.getjenny.starchat.entities._


trait TestEnglishBase extends TestBase {
  val createEnglishRequest = CreateLanguageIndexRequest(List("english"))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Post("/language_index_management", createEnglishRequest) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
    Post(s"/index_getjenny_english_0/index_management/create") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
    Post(s"/index_getjenny_english_common_0/index_management/create") ~> addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Delete("/language_index_management?index=index_english") ~>  addCredentials(testAdminCredentials) ~> routes ~> check {
      true
    }
  }

}
