package com.getjenny.starchat.resources

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.getjenny.starchat.serializers.JsonSupport
import com.getjenny.starchat.services.esclient.{EsCrudBase, IndexManagementElasticClient}
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.DocWriteResponse.Result
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

class EsCrudBaseTest extends FunSuite with Matchers with ScalatestRouteTest with JsonSupport with BeforeAndAfterAll {

  val client = IndexManagementElasticClient

  val indexName = "index_getjenny_test_0"
  val esSystemIndexName = Index.esLanguageFromIndexName(indexName, client.indexSuffix)
  val esCrudBase = EsCrudBase(client, indexName)

  override protected def beforeAll(): Unit = {
    val request = new CreateIndexRequest(esSystemIndexName)
    request.settings(Settings.builder()
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 1)
    );
    request.mapping(
      """{
        "properties": {
            "message": {"type": "keyword"},
            "instance": { "type": "keyword"}
            }
      }""",
      XContentType.JSON);
    import org.elasticsearch.client.RequestOptions
    client.httpClient.indices.create(request, RequestOptions.DEFAULT)
  }

  test("insert test") {
    val res = esCrudBase.index("instance", "1", Map("message" -> "ciao"))
    val res2 = esCrudBase.index("instance2", "2", Map("message" -> "aaaaa"))

    esCrudBase.refresh()

    assert(res.getResult === Result.CREATED)
    assert(res2.getResult === Result.CREATED)

  }

  test("find with match test") {

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("message", "ciao"))

    val findResponse = esCrudBase.find("instance", boolQueryBuilder)
    findResponse.getHits.forEach(println)

    val message = findResponse.getHits.getHits
      .flatMap(x => x.getSourceAsMap.asScala.get("message"))
      .map(_.asInstanceOf[String])
      .filter(x => x.equals("ciao"))

    assert(message.nonEmpty)
    assert(message.length == 1)
    assert(message.head.equals("ciao"))
  }

  test("find with match all test"){
    val query = QueryBuilders.matchAllQuery
    val findResponse = esCrudBase.find("instance2", query)
    findResponse.getHits.forEach(println)

    val message = findResponse.getHits.getHits
      .flatMap(x => x.getSourceAsMap.asScala.get("message"))
      .map(_.asInstanceOf[String])

    assert(message.nonEmpty)
    assert(message.length == 1)
    assert(message.head.equals("aaaaa"))
  }

  test("find all test") {
    val res: MultiGetResponse = esCrudBase.findAll(List("1", "2"))

    res.getResponses.map(_.getResponse.getSource).foreach(println)
  }

  /*test("delete test") {

    val boolQueryBuilder = QueryBuilders.boolQuery()

    val findResponse1 = esCrudBase.find("instance2", boolQueryBuilder)
    val delete = esCrudBase.deleteByQuery("instance2", boolQueryBuilder)
    esCrudBase.refresh()
    Thread.sleep(1000)
    val findResponse2 = esCrudBase.find("instance", boolQueryBuilder)

    findResponse1.getHits.forEach(println)
    println("-----------")
    findResponse2.getHits.forEach(println)


    val delete2 = esCrudBase.deleteByQuery("instance2", boolQueryBuilder)

    val findResponse3 = esCrudBase.find("instance2", boolQueryBuilder)
    val findResponse4 = esCrudBase.find("instance", boolQueryBuilder)

    findResponse3.getHits.forEach(println)
    findResponse4.getHits.forEach(println)

    esCrudBase.refresh()

    assert(delete.getDeleted === 1L)
    assert(delete2.getDeleted === 1L)
  }*/

  override protected def afterAll(): Unit = {
    val deleteIndexReq = new DeleteIndexRequest(esSystemIndexName)

    client.httpClient.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)
  }

}
