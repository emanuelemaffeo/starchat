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

  val indexName = "index_getjenny_english_test_0"
  val esLanguageSpecificIndexName: String = Index.esLanguageFromIndexName(indexName, client.indexSuffix)
  val indexLanguageCrud = IndexLanguageCrud(client, indexName)


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
    val res = indexLanguageCrud.create("instance", "1", document("instance", "ciao"))
    val res2 = indexLanguageCrud.create("instance2", "2", document("instance2", "aaaaa"))

    indexLanguageCrud.refresh()

    assert(res.getResult === Result.CREATED)
    assert(res2.getResult === Result.CREATED)

  }

  test("fail when trying to create a document evaluated with instance different to the one passed as parameter"){
    intercept[Exception] {
      indexLanguageCrud.create("instance2", "1", document("instance", "ciao"))
    }
  }

  test("insert with same id should fail test") {
    intercept[Exception] {
      indexLanguageCrud.create("instance", "1", document("instance", "ciao"))
    }
  }

  test("Update document with same id and different instance test") {
    intercept[Exception] {
      indexLanguageCrud.update("instance2", "1", document("instance2", "ciao2"))
    }
  }

  test("Bulk update document with same id and different instance test") {
    val documents = List(
      "1" -> document("instance2", "aaa"),
      "2" -> document("instance2", "bbbb")
    )
    intercept[Exception] {
      indexLanguageCrud.bulkUpdate("instance2", documents)
    }
  }

  test("find with match test") {

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("message", "ciao"))

    val findResponse = indexLanguageCrud.read("instance", boolQueryBuilder)
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
    val findResponse = indexLanguageCrud.read("instance2", query)
    findResponse.getHits.forEach(println)

    val message = findResponse.getHits.getHits
      .flatMap(x => x.getSourceAsMap.asScala.get("message"))
      .map(_.asInstanceOf[String])

    assert(message.nonEmpty)
    assert(message.length == 1)
    assert(message.head === "aaaaa")
  }

  test("find all test") {
    val res: MultiGetResponse = indexLanguageCrud.readAll(List("1", "2"))

    res.getResponses.map(_.getResponse.getSource).foreach(println)
  }

  test("delete test") {
    val boolQueryBuilder = QueryBuilders.matchAllQuery()

    val delete = indexLanguageCrud.delete("instance2", boolQueryBuilder)
    indexLanguageCrud.refresh()

    val delete2 = indexLanguageCrud.delete("instance", boolQueryBuilder)
    indexLanguageCrud.refresh()

    assert(delete.getDeleted === 1L)
    assert(delete2.getDeleted === 1L)
  }

  override protected def afterAll(): Unit = {
    val deleteIndexReq = new DeleteIndexRequest(esLanguageSpecificIndexName)

    client.httpClient.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)
  }

}
