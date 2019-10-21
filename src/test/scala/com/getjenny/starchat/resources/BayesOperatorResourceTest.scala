package com.getjenny.starchat.resources

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import com.getjenny.starchat.entities._
import com.getjenny.starchat.utils.Index

class BayesOperatorResourceTest extends TestEnglishBase {

  "StarChat" should {
    "return an HTTP code 201 when creating a new user" in {
      val user = User(
        id = "test_user",
        password = "3c98bf19cb962ac4cd0227142b3495ab1be46534061919f792254b80c0f3e566f7819cae73bdc616af0ff555f7460ac96d88d56338d659ebd93e2be858ce1cf9",
        salt = "salt",
        permissions = Map[String, Set[Permissions.Value]]("index_getjenny_english_0" -> Set(Permissions.read, Permissions.write))
      )
      Post(s"/user", user) ~> addCredentials(testAdminCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
      }
    }
  }


  /* To test the Bayes Operator we will the following two states
      S1:  Analyzer Condition = keyword("Keyword1")
           WQ = Keyword1 is good
           WQ = Keyword1 is wonderful
           WQ = Keyword2 is so so
      S2:  Analyzer Condition = keyword("Keyword2")
           WQ = Keyword1 is enough
           WQ = Keyword2 is quite good

      S1: BayesOperator result if applied on "I love Keyword1" = 2/3
      S2: BayesOperator result if applied on "I love Keyword1" = 0
      Winner State is S1 with score 0.6666

      S1: BayesOperator result if applied on "I love Keyword2" = 0
      S2: BayesOperator result if applied on "I love Keyword2" = 1/2
      Winner State is S2 with score 0.5000
   */

  it should {
    "return an HTTP code 200 when deleting all documents" in {
      Delete("/index_getjenny_english_0/decisiontable/all") ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should {
    "return an HTTP code 201 when creating a new document" in {
      val decisionTableRequest = DTDocument(
        state = "S1",
        executionOrder = 0,
        maxStateCount = 0,
        analyzer = "bayes(keyword(\"Keyword1\"))",
        queries = List("Keyword1 is good", "Keyword1 is wonderful", "Keyword2 is so so"),
        bubble = "Keyword1 is the winner",
        action = "",
        actionInput = Map(),
        stateData = Map(),
        successValue = "",
        failureValue = "",
        evaluationClass = Some("default"),
        version = None
      )

      val decisionTableRequest2 = DTDocument(
        state = "S2",
        executionOrder = 0,
        maxStateCount = 0,
        analyzer = "bayes(keyword(\"Keyword2\"))",
        queries = List("Keyword1 is enough", "Keyword2 is quite good"),
        bubble = "Keyword2 is the winner",
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
        val response = responseAs[IndexDocumentResult]
        response.created should be(true)
        response.id should be("S1")
        response.index should be("index_getjenny_english_0.state")
        response.version should be(1)
      }
      Post(s"/index_getjenny_english_0/decisiontable?refresh=1", decisionTableRequest2) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        val response = responseAs[IndexDocumentResult]
        response.created should be(true)
        response.id should be("S2")
        response.index should be("index_getjenny_english_0.state")
        response.version should be(1)
      }
    }
  }

  it should {
    "return an HTTP code 200 when triggering an update of the DecisionTable" in {
      Post("/index_getjenny_english_0/decisiontable/analyzer") ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[DTAnalyzerLoad]
      }
    }
  }

  it should {
    "return S1 with score 2/3 when get_next_response input is [I love Keyword1]" in {
      val request = ResponseRequestIn(
        conversationId = "conv_12345",
        traversedStates = None,
        userInput = Some(ResponseRequestInUserInput(
          text = Some("I love Keyword1"),
          img = None
        )),
        state = None,
        data = None,
        threshold = Some(0),
        evaluationClass = None,
        maxResults = Some(1),
        searchAlgorithm = Some(SearchAlgorithm.NGRAM2)
      )


      Post("/index_getjenny_english_0/get_next_response", request) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[List[ResponseRequestOut]]
        response.map(_.state) should contain only ("S1")
        response.map(_.score) should contain only (2.0 / 3.0)
      }

    }
  }
  it should {
    "return S2 with score 1/2 when get_next_response input is [I love Keyword2]" in {
      val request = ResponseRequestIn(
        conversationId = "conv_12345",
        traversedStates = None,
        userInput = Some(ResponseRequestInUserInput(
          text = Some("I love Keyword2"),
          img = None
        )),
        state = None,
        data = None,
        threshold = Some(0),
        evaluationClass = None,
        maxResults = Some(1),
        searchAlgorithm = Some(SearchAlgorithm.NGRAM2)
      )


      Post("/index_getjenny_english_0/get_next_response", request) ~> addCredentials(testUserCredentials) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val response = responseAs[List[ResponseRequestOut]]
        response.map(_.state) should contain only ("S2")
        response.map(_.score) should contain only (1.0 / 2.0)
      }

    }
  }


}


