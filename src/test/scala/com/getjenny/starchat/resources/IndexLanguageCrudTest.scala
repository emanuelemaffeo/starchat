package com.getjenny.starchat.resources

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.getjenny.starchat.serializers.JsonSupport
import com.getjenny.starchat.services.esclient.{IndexLanguageCrud, IndexManagementElasticClient}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.DocWriteResponse.Result
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentType}
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder

class IndexLanguageCrudTest extends FunSuite with Matchers with ScalatestRouteTest with JsonSupport with BeforeAndAfterAll {

  val client: IndexManagementElasticClient.type = IndexManagementElasticClient

  val indexName1 = "index_getjenny_english_test_0"
  val indexName2 = "index_getjenny_english_test_1"
  val instance1 = Index.instanceName(indexName1)
  val instance2 = Index.instanceName(indexName2)
  val esLanguageSpecificIndexName: String = Index.esLanguageFromIndexName(indexName1, client.indexSuffix)
  val indexLanguageCrud = IndexLanguageCrud(client, indexName1)
  val indexLanguageCrud2 = IndexLanguageCrud(client, indexName2)


  override protected def beforeAll(): Unit = {
    val request = new CreateIndexRequest(esLanguageSpecificIndexName)
    request.settings(Settings.builder()
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 1)
    )
    request.mapping(
      """{
        "properties": {
            "message": {"type": "keyword"},
            "instance": { "type": "keyword"}
            }
      }""",
      XContentType.JSON)
    import org.elasticsearch.client.RequestOptions
    client.httpClient.indices.create(request, RequestOptions.DEFAULT)
  }

  private[this] def document(instance: String, message: String): XContentBuilder = {
    val builder = jsonBuilder().startObject()
    builder.field("instance", instance)
      .field("message", message)
      .endObject()
    builder
  }

  test("insert test") {
    val res = indexLanguageCrud.create("1", document(instance1, "ciao"))
    val res2 = indexLanguageCrud.create("2", document(instance1, "aaaa"))
    val res3 = indexLanguageCrud2.create("3", document(instance2, "bbbb"))

    indexLanguageCrud.refresh()

    assert(res.getResult === Result.CREATED)
    assert(res2.getResult === Result.CREATED)
    assert(res3.getResult === Result.CREATED)

  }

  test("insert with same id should fail test") {
    val caught = intercept[Exception] {
      indexLanguageCrud.create("1", document(instance1, "ciao"))
    }
    caught.printStackTrace()
  }

  test("Update document with same id and different instance test") {
    val caught = intercept[Exception] {
      indexLanguageCrud2.update("1", document(instance2, "ciao2"))
    }
    caught.printStackTrace()
  }

  test("Bulk update document with same id and different instance test") {
    val documents = List(
      "1" -> document(instance2, "aaa"),
      "2" -> document(instance2, "bbbb")
    )
    val caught = intercept[Exception] {
      indexLanguageCrud2.bulkUpdate(documents)
    }
    caught.printStackTrace()
  }

  test("find with match test") {

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("message", "ciao"))

    val findResponse = indexLanguageCrud.read(boolQueryBuilder)
    findResponse.getHits.forEach(println)

    val message = findResponse.getHits.getHits
      .flatMap(x => x.getSourceAsMap.asScala.get("message"))
      .map(_.asInstanceOf[String])
      .filter(x => x === "ciao")

    assert(message.nonEmpty)
    assert(message.length == 1)
    assert(message.head === "ciao")
  }

  test("find with match all test") {
    val query = QueryBuilders.matchAllQuery
    val findResponse = indexLanguageCrud.read(query)
    findResponse.getHits.forEach(println)

    val message = findResponse.getHits.getHits
      .flatMap(x => x.getSourceAsMap.asScala.get("message"))
      .map(_.asInstanceOf[String])

    assert(message.nonEmpty)
    assert(message.length === 2)
  }

  test("find all test") {
    val res: MultiGetResponse = indexLanguageCrud.readAll(List("1", "2"))

    res.getResponses.map(_.getResponse.getSource).foreach(println)
    assert(res.getResponses.length === 2)
  }

  test("delete test") {
    val boolQueryBuilder = QueryBuilders.matchAllQuery()

    val delete = indexLanguageCrud.delete(boolQueryBuilder)
    indexLanguageCrud.refresh()

    assert(delete.getDeleted === 2L)
  }

  override protected def afterAll(): Unit = {
    val deleteIndexReq = new DeleteIndexRequest(esLanguageSpecificIndexName)

    client.httpClient.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)
  }

}
