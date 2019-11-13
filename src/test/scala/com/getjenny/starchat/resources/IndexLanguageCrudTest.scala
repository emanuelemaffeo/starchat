package com.getjenny.starchat.resources

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.getjenny.starchat.entities.es.EsEntityManager
import com.getjenny.starchat.serializers.JsonSupport
import com.getjenny.starchat.services.esclient.IndexManagementElasticClient
import com.getjenny.starchat.services.esclient.crud.IndexLanguageCrud
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentType}
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

case class TestDocument(id: String, message: String)

class IndexLanguageCrudTest extends FunSuite with Matchers with ScalatestRouteTest with JsonSupport with BeforeAndAfterAll {

  val client: IndexManagementElasticClient.type = IndexManagementElasticClient

  val indexName1: String = "index_getjenny_english_test_0"
  val indexName2: String = "index_getjenny_english_test_1"
  val instance1: String = Index.instanceName(indexName1)
  val instance2: String = Index.instanceName(indexName2)
  val esLanguageSpecificIndexName: String = Index.esLanguageFromIndexName(indexName1, client.indexSuffix)
  val indexLanguageCrud: IndexLanguageCrud = IndexLanguageCrud(client, indexName1)
  val indexLanguageCrud2: IndexLanguageCrud = IndexLanguageCrud(client, indexName2)

  object TestEntityManager extends EsEntityManager[TestDocument, TestDocument] {
    override def fromSearchResponse(response: SearchResponse): List[TestDocument] = {
      response.getHits.getHits.map(x => fromSource(x.getId, x.getSourceAsMap.asScala.toMap)).toList
    }

    override def fromGetResponse(response: List[GetResponse]): List[TestDocument] = {
      response.map(x => fromSource(x.getId, x.getSourceAsMap.asScala.toMap))
    }

    private[this] def fromSource(id: String, source: Map[String, Any]): TestDocument = {
      val message = source("message").toString
      TestDocument(extractId(id), message)
    }

    override def toXContentBuilder(entity: TestDocument, instance: String): (String, XContentBuilder) = {
      val builder = jsonBuilder().startObject()
      builder.field("instance", instance)
        .field("message", entity.message)
        .endObject()
      createId(instance, entity.id) -> builder
    }
  }

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

  test("insert test") {
    val res = indexLanguageCrud.create(TestDocument("1", "ciao"), TestEntityManager)
    val res2 = indexLanguageCrud.create(TestDocument("2", "aaaa"), TestEntityManager)
    val res3 = indexLanguageCrud2.create(TestDocument("3", "bbbb"), TestEntityManager)

    indexLanguageCrud.refresh(1)

    assert(res.created === true)
    assert(res2.created === true)
    assert(res3.created === true)

  }

  test("insert with same id should fail test") {
    val caught = intercept[Exception] {
      indexLanguageCrud.create(TestDocument("1", "ciao"), TestEntityManager)
    }
    caught.printStackTrace()
  }

  test("Update document with same id and different instance test") {
    val caught = intercept[Exception] {
      indexLanguageCrud2.update(TestDocument("1", "ciao2"), entityManager = TestEntityManager)
    }
    caught.printStackTrace()
  }

  test("Bulk update document with same id and different instance test") {
    val documents = List(
      TestDocument("1", "aaa"),
      TestDocument("2", "bbbb")
    )
    indexLanguageCrud2.bulkUpdate(documents.map(x => x.id -> x), entityManager = TestEntityManager)
    val res = indexLanguageCrud.readAll(List("1", "2"), TestEntityManager)

    assert(!res.exists(_.message === "aaa"))
    assert(!res.exists(_.message === "bbb"))
  }

  test("find with match test") {

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("message", "ciao"))

    val findResponse = indexLanguageCrud.read(boolQueryBuilder, entityManager = TestEntityManager)

    val message = findResponse.filter(x => x.message === "ciao")

    assert(message.nonEmpty)
    assert(message.length === 1)
    assert(message.headOption.map(_.message).getOrElse("") === "ciao")
  }

  test("find with match all test") {
    val query = QueryBuilders.matchAllQuery
    val findResponse = indexLanguageCrud.read(query, entityManager = TestEntityManager)

    val message = findResponse.map(_.message)

    assert(message.nonEmpty)
    assert(message.length === 2)
  }

  test("find all test") {
    val res = indexLanguageCrud.readAll(List("1", "2"), TestEntityManager)

    res.foreach(println)
    assert(res.length === 2)
    assert(res.exists(_.id === "1"))
    assert(res.exists(_.id === "2"))
  }

  test("delete test") {
    val boolQueryBuilder = QueryBuilders.matchAllQuery()

    val delete = indexLanguageCrud.delete(boolQueryBuilder)
    indexLanguageCrud.refresh(1)

    assert(delete.getDeleted === 2L)
  }

  override protected def afterAll(): Unit = {
    val deleteIndexReq = new DeleteIndexRequest(esLanguageSpecificIndexName)

    client.httpClient.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)
  }

}
