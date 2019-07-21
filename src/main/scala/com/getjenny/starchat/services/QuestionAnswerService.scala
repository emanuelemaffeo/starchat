package com.getjenny.starchat.services

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.analyzer.util.RandomNumbers
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities._
import com.getjenny.starchat.services.esclient.QuestionAnswerElasticClient
import com.getjenny.starchat.utils.Index
import org.apache.lucene.search.join._
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequest}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest, SearchType}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.functionscore._
import org.elasticsearch.index.query.{BoolQueryBuilder, InnerHitBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script._
import org.elasticsearch.search.builder.SearchSourceBuilder
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.Future

case class QuestionAnswerServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

trait QuestionAnswerService extends AbstractDataService {
  override val elasticClient: QuestionAnswerElasticClient
  val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  val nested_score_mode: Map[String, ScoreMode] = Map[String, ScoreMode]("min" -> ScoreMode.Min,
    "max" -> ScoreMode.Max, "avg" -> ScoreMode.Avg, "total" -> ScoreMode.Total)

  def documentFromMap(indexName: String, id: String, source : Map[String, Any]): KBDocument = {
    val conversation : String = source.get("conversation") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => throw QuestionAnswerServiceException("Missing conversation ID for " +
        "index:docId(" + indexName + ":"  + id + ")")
    }

    val indexInConversation : Option[Int] = source.get("index_in_conversation") match {
      case Some(t) => Some(t.asInstanceOf[Int])
      case _ => Some(0)
    }

    val status : Int = source.get("status") match {
      case Some(t) => t.asInstanceOf[Int]
      case _ => 0
    }

    val question : String = source.get("question") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => ""
    }

    val questionNegative : Option[List[String]] = source.get("question_negative") match {
      case Some(t) =>
        val res = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
          .asScala.map(_.asScala.get("query")).filter(_.nonEmpty).map(_.get).toList
        Option { res }
      case _ => None: Option[List[String]]
    }

    val questionScoredTerms: Option[List[(String, Double)]] = source.get("question_scored_terms") match {
      case Some(t) => Option {
        t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
          .map(pair =>
            (pair.getOrDefault("term", "").asInstanceOf[String],
              pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
          .toList
      }
      case _ => None : Option[List[(String, Double)]]
    }

    val answer : String = source.get("answer") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => ""
    }

    val answerScoredTerms: Option[List[(String, Double)]] = source.get("answer_scored_terms") match {
      case Some(t) => Option {
        t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
          .map(pair =>
            (pair.getOrDefault("term", "").asInstanceOf[String],
              pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
          .toList
      }
      case _ => None : Option[List[(String, Double)]]
    }

    val topics : Option[String] = source.get("topics") match {
      case Some(t) => Option { t.asInstanceOf[String] }
      case _ => None : Option[String]
    }

    val verified : Boolean = source.get("verified") match {
      case Some(t) => t.asInstanceOf[Boolean]
      case _ => false
    }

    val dclass : Option[String] = source.get("dclass") match {
      case Some(t) => Option { t.asInstanceOf[String] }
      case _ => None : Option[String]
    }

    val doctype : Doctypes.Value = source.get("doctype") match {
      case Some(t) => Doctypes.value(t.asInstanceOf[String])
      case _ => Doctypes.normal
    }

    val state : Option[String] = source.get("state") match {
      case Some(t) => Option { t.asInstanceOf[String] }
      case _ => None : Option[String]
    }

    KBDocument(
      id = id,
      conversation = conversation,
      index_in_conversation = indexInConversation,
      question = question,
      question_negative = questionNegative,
      question_scored_terms = questionScoredTerms,
      answer = answer,
      answer_scored_terms = answerScoredTerms,
      verified = verified,
      topics = topics,
      dclass = dclass,
      doctype = doctype,
      state = state,
      status = status
    )
  }

  private[this] def queryBuilder(documentSearch: KBDocumentSearch): BoolQueryBuilder = {
    val boolQueryBuilder : BoolQueryBuilder = QueryBuilders.boolQuery()

    documentSearch.conversation match {
      case Some(convIds) =>
        val convIdBoolQ = QueryBuilders.boolQuery()
        convIds.foreach { cId => convIdBoolQ.should(QueryBuilders.termQuery("conversation", cId)) }
        boolQueryBuilder.must(convIdBoolQ)
      case _ => ;
    }

    documentSearch.index_in_conversation match {
      case Some(value) =>
        boolQueryBuilder.must(QueryBuilders.matchQuery("index_in_conversation", value))
      case _ => ;
    }

    documentSearch.status match {
      case Some(status) => boolQueryBuilder.filter(QueryBuilders.termQuery("status", status))
      case _ => ;
    }

    documentSearch.random.filter(identity) match {
      case Some(true) =>
        val randomBuilder = new RandomScoreFunctionBuilder().seed(RandomNumbers.integer)
        val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(randomBuilder)
        boolQueryBuilder.must(functionScoreQuery)
      case _ => ;
    }

    documentSearch.question match {
      case Some(questionQuery) =>

        val questionPositiveQuery: QueryBuilder = QueryBuilders.boolQuery()
          .must(QueryBuilders.matchQuery("question.stem", questionQuery))
          .should(QueryBuilders.matchPhraseQuery("question.raw", questionQuery)
            .boost(elasticClient.questionExactMatchBoost))

        val questionNegativeNestedQuery: QueryBuilder = QueryBuilders.nestedQuery(
          "question_negative",
          QueryBuilders.matchQuery("question_negative.query.base", questionQuery)
            .minimumShouldMatch(elasticClient.questionNegativeMinimumMatch),
          ScoreMode.Total
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().setSize(100))

        boolQueryBuilder.should(
          QueryBuilders.boostingQuery(questionPositiveQuery,
            questionNegativeNestedQuery
          ).negativeBoost(elasticClient.questionNegativeBoost)
        )
      case _ => ;
    }

    documentSearch.question_scored_terms match {
      case Some(questionScoredTerms) =>
        val queryTerms = QueryBuilders.boolQuery()
          .should(QueryBuilders.matchQuery("question_scored_terms.term", questionScoredTerms))
        val script: Script = new Script("doc[\"question_scored_terms.score\"].value")
        val scriptFunction = new ScriptScoreFunctionBuilder(script)
        val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(queryTerms, scriptFunction)

        val nestedQuery: QueryBuilder = QueryBuilders.nestedQuery(
          "question_scored_terms",
          functionScoreQuery,
          nested_score_mode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Total)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100))
        boolQueryBuilder.should(nestedQuery)
      case _ => ;
    }

    documentSearch.answer match {
      case Some(value) =>
        boolQueryBuilder.must(QueryBuilders.matchQuery("answer.stem", value))
      case _ => ;
    }

    documentSearch.answer_scored_terms match {
      case Some(answerScoredTerms) =>
        val queryTerms = QueryBuilders.boolQuery()
          .should(QueryBuilders.matchQuery("answer_scored_terms.term", answerScoredTerms))
        val script: Script = new Script("doc[\"answer_scored_terms.score\"].value")
        val scriptFunction = new ScriptScoreFunctionBuilder(script)
        val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(queryTerms, scriptFunction)

        val nestedQuery: QueryBuilder = QueryBuilders.nestedQuery(
          "answer_scored_terms",
          functionScoreQuery,
          nested_score_mode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Total)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100))
        boolQueryBuilder.should(nestedQuery)
      case _ => ;
    }

    documentSearch.topics match {
      case Some(topics) => boolQueryBuilder.filter(QueryBuilders.termQuery("topics.base", topics))
      case _ => ;
    }

    documentSearch.verified match {
      case Some(verified) => boolQueryBuilder.filter(QueryBuilders.termQuery("verified", verified))
      case _ => ;
    }

    documentSearch.dclass match {
      case Some(dclass) => boolQueryBuilder.filter(QueryBuilders.termQuery("dclass", dclass))
      case _ => ;
    }

    documentSearch.doctype match {
      case Some(doctype) => boolQueryBuilder.filter(QueryBuilders.termQuery("doctype", doctype.toString))
      case _ => ;
    }

    documentSearch.state match {
      case Some(state) => boolQueryBuilder.filter(QueryBuilders.termQuery("state", state))
      case _ => ;
    }

    boolQueryBuilder
  }

  def search(indexName: String, documentSearch: KBDocumentSearch): Option[SearchKBDocumentsResults] = {
    val client: RestHighLevelClient = elasticClient.httpClient

    val sourceReq: SearchSourceBuilder = new SearchSourceBuilder()
      .from(documentSearch.from.getOrElse(0))
      .size(documentSearch.size.getOrElse(10))
      .minScore(documentSearch.min_score.getOrElse(Option{elasticClient.queryMinThreshold}.getOrElse(0.0f)))

    val searchReq = new SearchRequest(Index.indexName(indexName, elasticClient.indexSuffix))
      .source(sourceReq)
      .searchType(SearchType.DFS_QUERY_THEN_FETCH)

    val boolQueryBuilder : BoolQueryBuilder = queryBuilder(documentSearch)

    sourceReq.query(boolQueryBuilder)

    val searchResp: SearchResponse = client.search(searchReq, RequestOptions.DEFAULT)

    val documents : Option[List[SearchKBDocument]] = Option {
      searchResp.getHits.getHits.toList.map { item =>
        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val document = documentFromMap(indexName, id, source)

        val searchDocument : SearchKBDocument = SearchKBDocument(score = item.getScore, document = document)
        searchDocument
      }
    }

    val filteredDoc : List[SearchKBDocument] =
      documents.getOrElse(List.empty[SearchKBDocument])

    val maxScore : Float = searchResp.getHits.getMaxScore
    val totalHits = searchResp.getHits.getTotalHits.value
    val searchResults : SearchKBDocumentsResults = SearchKBDocumentsResults(
      total = totalHits,
      max_score = maxScore,
      hits = filteredDoc)

    Some(searchResults)
  }

  def create(indexName: String, document: KBDocument, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)
    builder.field("conversation", document.conversation)

    if (document.index_in_conversation.getOrElse(0) <= 0)
      throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
    builder.field("index_in_conversation", document.index_in_conversation)

    builder.field("status", document.status)
    builder.field("question", document.question)

    document.question_negative match {
      case Some(t) =>
        val array = builder.startArray("question_negative")
        t.foreach(q => {
          array.startObject().field("query", q).endObject()
        })
        array.endArray()
      case _ => ;
    }
    document.question_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("question_scored_terms")
        t.foreach { case (term, score) =>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case _ => ;
    }

    builder.field("answer", document.answer)

    document.answer_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("answer_scored_terms")
        t.foreach { case (term, score) =>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case _ => ;
    }
    document.topics match {
      case Some(t) => builder.field("topics", t)
      case _ => ;
    }

    builder.field("verified", document.verified)

    document.dclass match {
      case Some(t) => builder.field("dclass", t)
      case _ => ;
    }

    builder.field("doctype", document.doctype.toString)

    document.state match {
      case Some(t) => builder.field("state", t)
      case _ => ;
    }

    builder.endObject()

    val client: RestHighLevelClient = elasticClient.httpClient

    val indexReq = new IndexRequest()
      .index(Index.indexName(indexName, elasticClient.indexSuffix))
      .id(document.id)
      .source(builder)

    val response: IndexResponse = client.index(indexReq, RequestOptions.DEFAULT)

    if (refresh =/= 0) {
      val refresh_index = elasticClient.refresh(Index.indexName(indexName, elasticClient.indexSuffix))
      if(refresh_index.failed_shards_n > 0) {
        throw QuestionAnswerServiceException("index refresh failed: (" + indexName + ")")
      }
    }

    val doc_result: IndexDocumentResult = IndexDocumentResult(index = response.getIndex,
      id = response.getId,
      dtype = response.getType,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {doc_result}
  }

  private[this] def updateBuilder(document: KBDocumentUpdate): XContentBuilder = {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.conversation match {
      case Some(t) => builder.field("conversation", t)
      case _ => ;
    }

    document.index_in_conversation match {
      case Some(t) =>
        if (t <= 0) throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
        builder.field("indexInConversation", t)
      case _ => ;
    }

    document.status match {
      case Some(t) => builder.field("status", t)
      case _ => ;
    }

    document.question match {
      case Some(t) => builder.field("question", t)
      case _ => ;
    }
    document.question_negative match {
      case Some(t) =>
        val array = builder.startArray("question_negative")
        t.foreach(q => {
          array.startObject().field("query", q).endObject()
        })
        array.endArray()
      case _ => ;
    }
    document.question_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("question_scored_terms")
        t.foreach{case(term, score) =>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case _ => ;
    }
    document.answer match {
      case Some(t) => builder.field("answer", t)
      case _ => ;
    }
    document.answer_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("answer_scored_terms")
        t.foreach{case(term, score) =>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case _ => ;
    }
    document.topics match {
      case Some(t) => builder.field("topics", t)
      case _ => ;
    }
    document.verified match {
      case Some(t) => builder.field("verified", t)
      case _ => ;
    }
    document.dclass match {
      case Some(t) => builder.field("dclass", t)
      case _ => ;
    }
    builder.field("doctype", document.doctype.toString)
    document.state match {
      case Some(t) =>
        builder.field("state", t)
      case _ => ;
    }

    builder.endObject()
    builder
  }

  def update(indexName: String, id: String, document: KBDocumentUpdate, refresh: Int): UpdateDocumentResult = {
    val builder = updateBuilder(document)

    val client: RestHighLevelClient = elasticClient.httpClient

    val bulkRequest = new BulkRequest
    val updateReq = new UpdateRequest()
      .index(Index.indexName(indexName, elasticClient.indexSuffix))
      .doc(builder)
      .id(id)
    bulkRequest.add(updateReq)

    val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)

    if (refresh =/= 0) {
      val refresh_index = elasticClient.refresh(Index.indexName(indexName, elasticClient.indexSuffix))
      if(refresh_index.failed_shards_n > 0) {
        throw QuestionAnswerServiceException("index refresh failed: (" + indexName + ")")
      }
    }

    val listOfDocRes: List[UpdateDocumentResult] = bulkResponse.getItems.map(response => {
      UpdateDocumentResult(index = response.getIndex,
        id = response.getId,
        dtype = response.getType,
        version = response.getVersion,
        created = response.status === RestStatus.CREATED
      )
    }).toList

    listOfDocRes.head
  }

  def read(indexName: String, ids: List[String]): Option[SearchKBDocumentsResults] = {
    val client: RestHighLevelClient = elasticClient.httpClient

    val multigetReq = new MultiGetRequest()
    ids.foreach{ id =>
      multigetReq.add(
        new MultiGetRequest.Item(Index
          .indexName(indexName, elasticClient.indexSuffix), id)
      )
    }

    val documents : Option[List[SearchKBDocument]] = Some {
      client.mget(multigetReq, RequestOptions.DEFAULT).getResponses.toList
        .filter((p: MultiGetItemResponse) => p.getResponse.isExists).map { e =>

        val item: GetResponse = e.getResponse

        val id : String = item.getId

        val source : Map[String, Any] = item.getSource.asScala.toMap

        val document = documentFromMap(indexName, id, source)

        SearchKBDocument(score = .0f, document = document)
      }
    }

    val filteredDoc : List[SearchKBDocument] = documents.getOrElse(List[SearchKBDocument]())

    val maxScore : Float = .0f
    val total : Int = filteredDoc.length
    val searchResults : SearchKBDocumentsResults = SearchKBDocumentsResults(
      total = total,
      max_score = maxScore,
      hits = filteredDoc)

    val searchResultsOption : Option[SearchKBDocumentsResults] = Option { searchResults }
    searchResultsOption
  }

  def readFuture(indexName: String, ids: List[String]): Future[Option[SearchKBDocumentsResults]] = Future {
    read(indexName, ids)
  }

  def allDocuments(indexName: String, keepAlive: Long = 60000, size: Int = 100): Iterator[KBDocument] = {
    val client: RestHighLevelClient = elasticClient.httpClient

    val sourceReq: SearchSourceBuilder = new SearchSourceBuilder()
      .query(QueryBuilders.matchAllQuery)
      .size(size)

    val searchReq = new SearchRequest(Index.indexName(indexName, elasticClient.indexSuffix))
      .source(sourceReq)
      .scroll(new TimeValue(keepAlive))

    var scrollResp: SearchResponse = client.search(searchReq, RequestOptions.DEFAULT)
    val scrollId = scrollResp.getScrollId

    val iterator = Iterator.continually{
      val documents = scrollResp.getHits.getHits.toList.map { item =>
        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        documentFromMap(indexName, id, source)
      }

      var scrollRequest: SearchScrollRequest = new SearchScrollRequest(scrollId)
      scrollRequest.scroll(new TimeValue(keepAlive))
      scrollResp = client.scroll(scrollRequest, RequestOptions.DEFAULT)
      (documents, documents.nonEmpty)
    }.takeWhile{case (_, docNonEmpty) => docNonEmpty}
    iterator
      .flatMap{case (doc, _) => doc}
  }
}
