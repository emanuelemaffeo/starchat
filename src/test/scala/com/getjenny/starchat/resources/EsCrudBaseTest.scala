package com.getjenny.starchat.resources

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.getjenny.starchat.serializers.JsonSupport
import com.getjenny.starchat.services.esclient.IndexManagementElasticClient
import com.getjenny.starchat.utils.EsCrudBase
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.collection.JavaConverters._

class EsCrudBaseTest extends FunSuite with Matchers with ScalatestRouteTest with JsonSupport with BeforeAndAfterAll {

  val client = IndexManagementElasticClient

  val indexName = "test_index"

  val esCrudBase = new EsCrudBase(client, indexName)

  override protected def beforeAll(): Unit = {
    val request = new CreateIndexRequest(indexName)
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

  test("upsert test") {
    val res = esCrudBase.update("prova", "asasd3f", Map("message" -> "ciao"))

    println(res.status())
  }

  test("find test") {
    Thread.sleep(1000)

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .filter(QueryBuilders.termQuery("message", "ciao"))

    val res = esCrudBase.find("prova", boolQueryBuilder, "message")

    res.getHits.forEach(println)
  }

  test("delete test") {
    Thread.sleep(1000)

    val boolQueryBuilder = QueryBuilders.boolQuery()
      .filter(QueryBuilders.matchAllQuery())

    val res = esCrudBase.deleteByQuery("prova", boolQueryBuilder)

    println(res.getDeleted)
  }

  override protected def afterAll(): Unit = {
     val deleteIndexReq = new DeleteIndexRequest(indexName)

     client.httpClient.indices.delete(deleteIndexReq, RequestOptions.DEFAULT)
   }

}
