package com.getjenny.starchat.services

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

import java.time.{ZoneId, ZoneOffset, ZonedDateTime}

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.analyzer.util.{RandomNumbers, Time}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.entities.{LabelCountHistogramItem, _}
import com.getjenny.starchat.services.DecisionTableService.elasticClient
import com.getjenny.starchat.services.esclient.{IndexLanguageCrud, QuestionAnswerElasticClient}
import com.getjenny.starchat.utils.Index
import org.apache.lucene.search.join._
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.{SearchScrollRequest, SearchType}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.functionscore._
import org.elasticsearch.index.query.{BoolQueryBuilder, InnerHitBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script._
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter
import org.elasticsearch.search.aggregations.bucket.histogram.{DateHistogramInterval, Histogram, ParsedDateHistogram}
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.elasticsearch.search.aggregations.metrics.{Avg, Cardinality, Sum}
import org.elasticsearch.search.aggregations.{AggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.sort.{FieldSortBuilder, ScoreSortBuilder, SortOrder}
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class QuestionAnswerServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

trait QuestionAnswerService extends AbstractDataService {
  override val elasticClient: QuestionAnswerElasticClient

  val manausTermsExtractionService: ManausTermsExtractionService.type = ManausTermsExtractionService
  val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  val nested_score_mode: Map[String, ScoreMode] = Map[String, ScoreMode]("min" -> ScoreMode.Min,
    "max" -> ScoreMode.Max, "avg" -> ScoreMode.Avg, "total" -> ScoreMode.Total)

  private[this] val intervalRe = """(?:([1-9][0-9]*)([ms|m|h|d|w|M|q|y]{1}))""".r

  var cacheStealTimeMillis: Int

  /** Calculate the dictionary size for one index i.e. the number of unique terms
    * in the fields question, answer and in the union of both the fields
    *
    * @param indexName the index name
    * @return a data structure with the unique terms counts
    */
  private[this] def calcDictSize(indexName: String): DictSize = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val questionAgg = AggregationBuilders.cardinality("question_term_count").field("question.base")
    val answerAgg = AggregationBuilders.cardinality("answer_term_count").field("answer.base")
    val scriptBody = "def qnList = new ArrayList(doc[\"question.base\"]) ; " +
      "List anList = doc[\"answer.base\"] ; qnList.addAll(anList) ; return qnList ;"
    val totalAgg = AggregationBuilders.cardinality("total_term_count").script(new Script(scriptBody))

    val query = QueryBuilders.matchAllQuery

    val searchResp = indexLanguageCrud.read(instance, query,
      aggregation = List(questionAgg, answerAgg, totalAgg),
      searchType = SearchType.DFS_QUERY_THEN_FETCH,
      maxItems = Option(0),
      requestCache = Option(true))

    val questionAggRes: Cardinality = searchResp.getAggregations.get("question_term_count")
    val answerAggRes: Cardinality = searchResp.getAggregations.get("answer_term_count")
    val totalAggRes: Cardinality = searchResp.getAggregations.get("total_term_count")

    DictSize(numDocs = searchResp.getHits.getTotalHits.value,
      question = questionAggRes.getValue,
      answer = answerAggRes.getValue,
      total = totalAggRes.getValue
    )
  }

  var dictSizeCacheMaxSize: Int
  private[this] val dictSizeCache: mutable.LinkedHashMap[String, (Long, DictSize)] =
    mutable.LinkedHashMap[String, (Long, DictSize)]()

  /** Return the the dictionary size for one index i.e. the number of unique terms
    * in the fields question, answer and in the union of both the fields
    * The function returns cached results.
    *
    * @param indexName the index name
    * @param stale     the max cache age in milliseconds
    * @return a data structure with the unique terms counts
    */
  def dictSize(indexName: String, stale: Long = cacheStealTimeMillis): DictSize = {
    val key = indexName
    dictSizeCache.get(key) match {
      case Some((lastUpdateTs, dictSize)) =>
        val cacheStaleTime = math.abs(Time.timestampMillis - lastUpdateTs)
        if (cacheStaleTime < stale) {
          dictSize
        } else {
          val result = calcDictSize(indexName = indexName)
          if (dictSizeCache.size >= dictSizeCacheMaxSize) {
            dictSizeCache.head match {
              case (oldestTerm, (_, _)) => dictSizeCache -= oldestTerm
            }
          }
          dictSizeCache.remove(key)
          dictSizeCache.update(key, (Time.timestampMillis, result))
          result
        }
      case _ =>
        val result = calcDictSize(indexName = indexName)
        if (dictSizeCache.size >= dictSizeCacheMaxSize) {
          dictSizeCache.head match {
            case (oldestTerm, (_, _)) => dictSizeCache -= oldestTerm
          }
        }
        dictSizeCache.update(key, (Time.timestampMillis, result))
        result
    }
  }

  /** Calculate the total number of terms in the fields question and answer, including duplicates.
    *
    * @param indexName the index name
    * @return a data structure with the terms counting and the total number of documents
    */
  private[this] def calcTotalTerms(indexName: String): TotalTerms = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val questionAgg = AggregationBuilders.sum("question_term_count").field("question.base_length")
    val answerAgg = AggregationBuilders.sum("answer_term_count").field("answer.base_length")

    val query = QueryBuilders.matchAllQuery

    val searchResp = indexLanguageCrud.read(instance, query, aggregation = List(questionAgg, answerAgg),
      maxItems = Option(0),
      searchType = SearchType.DFS_QUERY_THEN_FETCH,
      requestCache = Option(true))

    val totalHits = searchResp.getHits.getTotalHits.value

    val questionAggRes: Sum = searchResp.getAggregations.get("question_term_count")
    val answerAggRes: Sum = searchResp.getAggregations.get("answer_term_count")

    TotalTerms(numDocs = totalHits,
      question = questionAggRes.getValue.toLong,
      answer = answerAggRes.getValue.toLong)
  }

  var totalTermsCacheMaxSize: Int
  private[this] val totalTermsCache: mutable.LinkedHashMap[String, (Long, TotalTerms)] =
    mutable.LinkedHashMap[String, (Long, TotalTerms)]()

  /** Returns the total number of terms in the fields question and answer, including duplicates.
    *
    * @param indexName the index name
    * @param stale     the max cache age in milliseconds
    * @return a data structure with the terms counting and the total number of documents
    */
  def totalTerms(indexName: String, stale: Long = cacheStealTimeMillis): TotalTerms = {
    val key = indexName
    totalTermsCache.get(key) match {
      case Some((lastUpdateTs, dictSize)) =>
        val cacheStaleTime = math.abs(Time.timestampMillis - lastUpdateTs)
        if (cacheStaleTime < stale) {
          dictSize
        } else {
          val result = calcTotalTerms(indexName = indexName)
          if (totalTermsCache.size >= totalTermsCacheMaxSize) {
            totalTermsCache.head match {
              case (oldestTerm, (_, _)) => totalTermsCache -= oldestTerm
            }
          }
          totalTermsCache.remove(key)
          totalTermsCache.update(key, (Time.timestampMillis, result))
          result
        }
      case _ =>
        val result = calcTotalTerms(indexName = indexName)
        if (totalTermsCache.size >= totalTermsCacheMaxSize) {
          totalTermsCache.head match {
            case (oldestTerm, (_, _)) => totalTermsCache -= oldestTerm
          }
        }
        totalTermsCache.update(key, (Time.timestampMillis, result))
        result
    }
  }

  /** calculate the occurrence of a term in the document fields questions or answer and the number of document
    * in which the term occur
    *
    * @param indexName index name
    * @param field     the field: question, answer or all for both
    * @param term      the term to search
    * @return the occurrence of term in the documents and the number of documents
    */
  def calcTermCount(indexName: String,
                    field: TermCountFields.Value = TermCountFields.question, term: String): TermCount = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val agg = AggregationBuilders.sum("countTerms")
      .script(new Script("_score"))

    val esFieldName: String = field match {
      case TermCountFields.question => "question.freq"
      case TermCountFields.answer => "answer.freq"
    }

    val query = QueryBuilders.matchQuery(esFieldName, term)

    val searchResp = indexLanguageCrud.read(instance, query, aggregation = List(agg),
      searchType = SearchType.DFS_QUERY_THEN_FETCH, requestCache = Option(true))

    val totalHits = searchResp.getHits.getTotalHits.value
    val aggRes: Sum = searchResp.getAggregations.get("countTerms")

    TermCount(numDocs = totalHits, count = aggRes.getValue.toLong)
  }

  var countTermCacheMaxSize: Int
  private[this] val countTermCache: mutable.LinkedHashMap[String, (Long, TermCount)] =
    mutable.LinkedHashMap[String, (Long, TermCount)]()

  /** Return the occurrence of a term in the document fields questions or answer and the number of document
    * in which the term occur
    *
    * @param indexName index name
    * @param field     the field: question, answer or all for both
    * @param term      the term to search
    * @param stale     the max cache age in milliseconds
    * @return the occurrence of term in the documents and the number of documents
    */
  def termCount(indexName: String, field: TermCountFields.Value, term: String,
                stale: Long = cacheStealTimeMillis): TermCount = {
    val key = indexName + field + term

    countTermCache.get(key)
      .filter { case (lastUpdateTs, _) => math.abs(Time.timestampMillis - lastUpdateTs) < stale }
      .map { case (_, dictSize) => dictSize }
      .getOrElse {
        val result = calcTermCount(indexName = indexName, field = field, term = term)
        if (countTermCache.size > countTermCacheMaxSize) {
          countTermCache.head match {
            case (oldestTerm, (_, _)) => countTermCache -= oldestTerm
          }
        }
        countTermCache.remove(key)
        countTermCache.update(key, (Time.timestampMillis, result))
        result
      }
  }

  /** set the number of terms counter's cached entries
    *
    * @param parameters the max number of cache entries
    * @return the parameters set
    */
  def countersCacheParameters(parameters: CountersCacheParameters): CountersCacheParameters = {
    parameters.dictSizeCacheMaxSize match {
      case Some(v) => this.dictSizeCacheMaxSize = v
      case _ => ;
    }

    parameters.totalTermsCacheMaxSize match {
      case Some(v) => this.totalTermsCacheMaxSize = v
      case _ => ;
    }

    parameters.countTermCacheMaxSize match {
      case Some(v) => this.countTermCacheMaxSize = v
      case _ => ;
    }

    parameters.cacheStealTimeMillis match {
      case Some(v) => this.cacheStealTimeMillis = v
      case _ => ;
    }

    CountersCacheParameters(
      dictSizeCacheMaxSize = Some(dictSizeCacheMaxSize),
      totalTermsCacheMaxSize = Some(totalTermsCacheMaxSize),
      countTermCacheMaxSize = Some(countTermCacheMaxSize),
      cacheStealTimeMillis = Some(cacheStealTimeMillis)
    )
  }

  def countersCacheParameters: (CountersCacheParameters, CountersCacheSize) = {
    (CountersCacheParameters(
      dictSizeCacheMaxSize = Some(dictSizeCacheMaxSize),
      totalTermsCacheMaxSize = Some(totalTermsCacheMaxSize),
      countTermCacheMaxSize = Some(countTermCacheMaxSize),
      cacheStealTimeMillis = Some(cacheStealTimeMillis)
    ),
      CountersCacheSize(
        dictSizeCacheSize = dictSizeCache.size,
        totalTermsCacheSize = totalTermsCache.size,
        countTermCacheSize = countTermCache.size
      ))
  }


  def countersCacheReset: (CountersCacheParameters, CountersCacheSize) = {
    dictSizeCache.clear()
    countTermCache.clear()
    totalTermsCache.clear()
    countersCacheParameters
  }

  def documentFromMap(indexName: String, id: String, source: Map[String, Any]): QADocument = {
    val conversation: String = source.get("conversation") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => throw QuestionAnswerServiceException("Missing conversation ID for " +
        "index:docId(" + indexName + ":" + id + ")")
    }

    val indexInConversation: Int = source.get("index_in_conversation") match {
      case Some(t) => t.asInstanceOf[Int]
      case _ => throw QuestionAnswerServiceException("Missing index in conversation for " +
        "index:docId(" + indexName + ":" + id + ")")
    }

    val status: Option[Int] = source.get("status") match {
      case Some(t) => Some(t.asInstanceOf[Int])
      case _ => Some(0)
    }

    val timestamp: Option[Long] = source.get("timestamp") match {
      case Some(t) => Option {
        t.asInstanceOf[Long]
      }
      case _ => None: Option[Long]
    }

    // begin core data
    val question: Option[String] = source.get("question") match {
      case Some(t) => Some(t.asInstanceOf[String])
      case _ => None
    }

    val questionNegative: Option[List[String]] = source.get("question_negative") match {
      case Some(t) =>
        val res = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
          .asScala.map(_.asScala.get("query")).filter(_.nonEmpty).map(_.get).toList
        Option {
          res
        }
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
      case _ => None: Option[List[(String, Double)]]
    }

    val answer: Option[String] = source.get("answer") match {
      case Some(t) => Some(t.asInstanceOf[String])
      case _ => None
    }

    val answerScoredTerms: Option[List[(String, Double)]] = source.get("answer_scored_terms") match {
      case Some(t) => Option {
        t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
          .map(pair =>
            (pair.getOrDefault("term", "").asInstanceOf[String],
              pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
          .toList
      }
      case _ => None: Option[List[(String, Double)]]
    }

    val topics: Option[String] = source.get("topics") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val verified: Option[Boolean] = source.get("verified") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val done: Option[Boolean] = source.get("done") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val coreDataOut: Option[QADocumentCore] = Some {
      QADocumentCore(
        question = question,
        questionNegative = questionNegative,
        questionScoredTerms = questionScoredTerms,
        answer = answer,
        answerScoredTerms = answerScoredTerms,
        topics = topics,
        verified = verified,
        done = done
      )
    }
    // begin core data

    // begin annotations
    val dclass: Option[String] = source.get("dclass") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val doctype: Option[Doctypes.Value] = source.get("doctype") match {
      case Some(t) => Some {
        Doctypes.value(t.asInstanceOf[String])
      }
      case _ => Some {
        Doctypes.NORMAL
      }
    }

    val state: Option[String] = source.get("state") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val agent: Option[Agent.Value] = source.get("agent") match {
      case Some(t) => Some(Agent.value(t.asInstanceOf[String]))
      case _ => Some(Agent.STARCHAT)
    }

    val escalated: Option[Escalated.Value] = source.get("escalated") match {
      case Some(t) => Some(Escalated.value(t.asInstanceOf[String]))
      case _ => Some(Escalated.UNSPECIFIED)
    }

    val answered: Option[Answered.Value] = source.get("answered") match {
      case Some(t) => Some(Answered.value(t.asInstanceOf[String]))
      case _ => Some(Answered.ANSWERED)
    }

    val triggered: Option[Triggered.Value] = source.get("triggered") match {
      case Some(t) => Some(Triggered.value(t.asInstanceOf[String]))
      case _ => Some(Triggered.UNSPECIFIED)
    }

    val followup: Option[Followup.Value] = source.get("followup") match {
      case Some(t) => Some(Followup.value(t.asInstanceOf[String]))
      case _ => Some(Followup.UNSPECIFIED)
    }

    val feedbackConv: Option[String] = source.get("feedbackConv") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val feedbackConvScore: Option[Double] = source.get("feedbackConvScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val algorithmConvScore: Option[Double] = source.get("algorithmConvScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val feedbackAnswerScore: Option[Double] = source.get("feedbackAnswerScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val algorithmAnswerScore: Option[Double] = source.get("algorithmAnswerScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val responseScore: Option[Double] = source.get("responseScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val start: Option[Boolean] = source.get("start") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val annotationsOut: Option[QADocumentAnnotations] = Some {
      QADocumentAnnotations(
        dclass = dclass,
        doctype = doctype,
        state = state,
        agent = agent,
        escalated = escalated,
        answered = answered,
        triggered = triggered,
        followup = followup,
        feedbackConv = feedbackConv,
        feedbackConvScore = feedbackConvScore,
        algorithmConvScore = algorithmConvScore,
        feedbackAnswerScore = feedbackAnswerScore,
        algorithmAnswerScore = algorithmAnswerScore,
        responseScore = responseScore,
        start = start
      )
    }
    // end annotations

    QADocument(
      id = id,
      conversation = conversation,
      indexInConversation = indexInConversation,
      status = status,
      coreData = coreDataOut,
      annotations = annotationsOut,
      timestamp = timestamp
    )
  }

  private[this] def queryBuilder(documentSearch: QADocumentSearch): BoolQueryBuilder = {
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()

    documentSearch.conversation match {
      case Some(convIds) =>
        val convIdBoolQ = QueryBuilders.boolQuery()
        convIds.foreach { cId => convIdBoolQ.should(QueryBuilders.termQuery("conversation", cId)) }
        boolQueryBuilder.must(convIdBoolQ)
      case _ => ;
    }

    documentSearch.indexInConversation match {
      case Some(value) =>
        boolQueryBuilder.must(QueryBuilders.matchQuery("index_in_conversation", value))
      case _ => ;
    }

    documentSearch.status match {
      case Some(status) => boolQueryBuilder.filter(QueryBuilders.termQuery("status", status))
      case _ => ;
    }

    documentSearch.timestampGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("timestamp")
            .gte(ts))
      case _ => ;
    }

    documentSearch.timestampLte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("timestamp")
            .lte(ts))
      case _ => ;
    }

    documentSearch.random.filter(identity) match {
      case Some(true) =>
        val randomBuilder = new RandomScoreFunctionBuilder().seed(RandomNumbers.integer)
        val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(randomBuilder)
        boolQueryBuilder.must(functionScoreQuery)
      case _ => ;
    }

    val coreDataIn = documentSearch.coreData.getOrElse(QADocumentCore())
    val annotationsIn = documentSearch.annotations.getOrElse(QADocumentAnnotationsSearch())

    // begin core data
    coreDataIn.question match {
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

    coreDataIn.questionScoredTerms match {
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

    coreDataIn.answer match {
      case Some(value) =>
        boolQueryBuilder.must(QueryBuilders.matchQuery("answer.stem", value))
      case _ => ;
    }

    coreDataIn.answerScoredTerms match {
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

    coreDataIn.topics match {
      case Some(topics) => boolQueryBuilder.filter(QueryBuilders.termQuery("topics.base", topics))
      case _ => ;
    }

    coreDataIn.verified match {
      case Some(verified) => boolQueryBuilder.filter(QueryBuilders.termQuery("verified", verified))
      case _ => ;
    }

    coreDataIn.done match {
      case Some(done) => boolQueryBuilder.filter(QueryBuilders.termQuery("done", done))
      case _ => ;
    }
    // end core data

    // begin annotations
    annotationsIn.dclass match {
      case Some(dclass) => boolQueryBuilder.filter(QueryBuilders.termQuery("dclass", dclass))
      case _ => ;
    }

    annotationsIn.doctype match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("doctype", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.state match {
      case Some(state) => boolQueryBuilder.filter(QueryBuilders.termQuery("state", state))
      case _ => ;
    }

    annotationsIn.agent match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("agent", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.escalated match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("escalated", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.answered match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("answered", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.triggered match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("triggered", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.followup match {
      case Some(queryParam) =>
        val orQuery = QueryBuilders.boolQuery()
        queryParam.foreach(i => orQuery.should(QueryBuilders.termQuery("followup", i.toString)))
        boolQueryBuilder.filter(orQuery)
      case _ => ;
    }

    annotationsIn.feedbackConv match {
      case Some(feedbackConv) => boolQueryBuilder.filter(QueryBuilders.termQuery("feedbackConv", feedbackConv))
      case _ => ;
    }

    annotationsIn.feedbackScoreConvGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("feedbackConvScore")
            .gte(ts))
      case _ => ;
    }

    annotationsIn.feedbackScoreConvGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("feedbackConvScore")
            .lte(ts))
      case _ => ;
    }

    annotationsIn.algorithmScoreConvGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("algorithmConvScore")
            .gte(ts))
      case _ => ;
    }

    annotationsIn.algorithmScoreConvLte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("algorithmConvScore")
            .lte(ts))
      case _ => ;
    }

    annotationsIn.feedbackScoreAnswerGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("feedbackAnswerScore")
            .gte(ts))
      case _ => ;
    }

    annotationsIn.feedbackScoreAnswerLte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("feedbackAnswerScore")
            .lte(ts))
      case _ => ;
    }

    annotationsIn.algorithmScoreAnswerGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("algorithmAnswerScore")
            .gte(ts))
      case _ => ;
    }

    annotationsIn.algorithmScoreAnswerLte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("algorithmAnswerScore")
            .lte(ts))
      case _ => ;
    }

    annotationsIn.responseScoreGte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("responseScore")
            .gte(ts))
      case _ => ;
    }

    annotationsIn.responseScoreLte match {
      case Some(ts) =>
        boolQueryBuilder.filter(
          QueryBuilders.rangeQuery("responseScore")
            .lte(ts))
      case _ => ;
    }

    annotationsIn.start match {
      case Some(start) => boolQueryBuilder.filter(QueryBuilders.termQuery("start", start))
      case _ => ;
    }
    // end annotations

    boolQueryBuilder
  }

  def search(indexName: String, documentSearch: QADocumentSearch): Option[SearchQADocumentsResults] = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val sort = documentSearch.sortByConvIdIdx match {
      case Some(true) => List(new FieldSortBuilder("conversation").order(SortOrder.DESC),
        new FieldSortBuilder("index_in_conversation").order(SortOrder.DESC),
        new FieldSortBuilder("timestamp").order(SortOrder.DESC))
      case _ => List(new ScoreSortBuilder().order(SortOrder.DESC))
    }

    val query = queryBuilder(documentSearch)
    val searchResp = indexLanguageCrud.read(instance, query,
      from = documentSearch.from,
      maxItems = documentSearch.size.orElse(Option(10)),
      minScore = documentSearch.minScore.orElse(Option(elasticClient.queryMinThreshold)),
      sort = sort,
      searchType = SearchType.DFS_QUERY_THEN_FETCH)

    val documents: Option[List[SearchQADocument]] = Option {
      searchResp.getHits.getHits.toList.map { item =>
        val id: String = item.getId
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        val document = documentFromMap(indexName, id, source)
        SearchQADocument(score = item.getScore, document = document)
      }
    }

    val filteredDoc: List[SearchQADocument] =
      documents.getOrElse(List.empty[SearchQADocument])

    val maxScore: Float = searchResp.getHits.getMaxScore
    val totalHits = searchResp.getHits.getTotalHits.value
    val total: Int = filteredDoc.length
    val searchResults: SearchQADocumentsResults = SearchQADocumentsResults(totalHits = totalHits,
      hitsCount = total, maxScore = maxScore, hits = filteredDoc)

    Some(searchResults)
  }

  def conversations(indexName: String, ids: DocsIds): Conversations = {
    val documentSearch = QADocumentSearch(
      from = Some(0),
      size = Some(10000),
      conversation = Some(ids.ids)
    )
    search(indexName = indexName, documentSearch = documentSearch) match {
      case Some(searchRes) =>
        val conversations = searchRes.hits.groupBy(_.document.conversation)
          .map { case (_: String, docs: List[SearchQADocument]) =>
            Conversation(
              count = docs.length,
              docs = docs.map((_: SearchQADocument).document)
                .sortBy(document => document.indexInConversation)
            )
          }.toList
        Conversations(total = conversations.length, conversations = conversations)
      case _ => Conversations()
    }
  }

  def analytics(indexName: String, request: QAAggregatedAnalyticsRequest): QAAggregatedAnalytics = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val firstIndexInConv: Long = 1
    val query: BoolQueryBuilder = QueryBuilders.boolQuery()

    val dateHistInterval = if (intervalRe.pattern.matcher(request.interval.getOrElse("1M")).matches()) {
      new DateHistogramInterval(request.interval.getOrElse("1M"))
    } else throw QuestionAnswerServiceException("time interval is not well formed")

    val minDocInBuckets = request.minDocInBuckets match {
      case Some(n) => n
      case _ => 1
    }

    request.timestampGte match {
      case Some(ts) =>
        query.filter(
          QueryBuilders.rangeQuery("timestamp")
            .gte(ts))
      case _ => ;
    }

    request.timestampLte match {
      case Some(ts) =>
        query.filter(
          QueryBuilders.rangeQuery("timestamp")
            .lte(ts))
      case _ => ;
    }

    val aggregationList = createAggregations(request, firstIndexInConv, dateHistInterval, minDocInBuckets)

    val searchResp = indexLanguageCrud.read(instance, query,
      searchType = SearchType.DFS_QUERY_THEN_FETCH,
      requestCache = Some(true),
      aggregation = aggregationList,
      maxItems = Option(0),
      minScore = Option(0.0f))

    val totalDocuments: Cardinality = searchResp.getAggregations.get("totalDocuments")
    val totalConversations: Cardinality = searchResp.getAggregations.get("totalConversations")

    val res = request.aggregations match {
      case Some(aggregationsReq) =>
        val reqAggs = aggregationsReq.toSet
        val avgFeedbackConvScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackConvScore)) {
          val avg: Avg = searchResp.getAggregations.get("avgFeedbackConvScore")
          Some(avg.getValue)
        } else None
        val avgFeedbackAnswerScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackAnswerScore)) {
          val avg: Avg = searchResp.getAggregations.get("avgFeedbackAnswerScore")
          Some(avg.getValue)
        } else None
        val avgAlgorithmConvScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmConvScore)) {
          val avg: Avg = searchResp.getAggregations.get("avgAlgorithmConvScore")
          Some(avg.getValue)
        } else None
        val avgAlgorithmAnswerScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmAnswerScore)) {
          val avg: Avg = searchResp.getAggregations.get("avgAlgorithmAnswerScore")
          Some(avg.getValue)
        } else None
        val scoreHistogram: Option[List[ScoreHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.scoreHistogram)) {
          val h: Histogram = searchResp.getAggregations.get("scoreHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              ScoreHistogramItem(
                key = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val scoreHistogramNotTransferred: Option[List[ScoreHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.scoreHistogramNotTransferred)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("scoreHistogramNotTransferred")
          val h: Histogram = pf.getAggregations.get("scoreHistogramNotTransferred")
          Some {
            h.getBuckets.asScala.map { bucket =>
              ScoreHistogramItem(
                key = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val scoreHistogramTransferred: Option[List[ScoreHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.scoreHistogramTransferred)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("scoreHistogramTransferred")
          val h: Histogram = pf.getAggregations.get("scoreHistogramTransferred")
          Some {
            h.getBuckets.asScala.map { bucket =>
              ScoreHistogramItem(
                key = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val conversationsHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.conversationsHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("conversationsHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("conversationsHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val conversationsNotTransferredHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.conversationsNotTransferredHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("conversationsNotTransferredHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("conversationsNotTransferredHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val conversationsTransferredHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.conversationsTransferredHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("conversationsTransferredHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("conversationsTransferredHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val qaPairHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.qaPairHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("qaPairHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("qaPairHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val qaPairAnsweredHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.qaPairAnsweredHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("qaPairAnsweredHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("qaPairAnsweredHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val qaPairUnansweredHistogram: Option[List[CountOverTimeHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.qaPairUnansweredHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("qaPairUnansweredHistogram")
          val h: ParsedDateHistogram = pf.getAggregations.get("qaPairUnansweredHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              CountOverTimeHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val qaMatchedStatesHistogram: Option[List[LabelCountHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.qaMatchedStatesHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("qaMatchedStatesHistogram")
          val h: ParsedStringTerms = pf.getAggregations.get("qaMatchedStatesHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              LabelCountHistogramItem(
                key = bucket.getKey.asInstanceOf[String],
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val qaMatchedStatesWithScoreHistogram: Option[List[LabelCountHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.qaMatchedStatesWithScoreHistogram)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("qaMatchedStatesWithScoreHistogram")
          val h: ParsedStringTerms = pf.getAggregations.get("qaMatchedStatesWithScoreHistogram")
          Some {
            h.getBuckets.asScala.map { bucket =>
              LabelCountHistogramItem(
                key = bucket.getKey.asInstanceOf[String],
                docCount = bucket.getDocCount
              )
            }.toList
          }
        } else None
        val avgFeedbackNotTransferredConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackNotTransferredConvScoreOverTime)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("avgFeedbackNotTransferredConvScoreOverTime")
          val h: ParsedDateHistogram = pf.getAggregations.get("avgFeedbackNotTransferredConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgFeedbackTransferredConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackTransferredConvScoreOverTime)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("avgFeedbackTransferredConvScoreOverTime")
          val h: ParsedDateHistogram = pf.getAggregations.get("avgFeedbackTransferredConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgAlgorithmNotTransferredConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmNotTransferredConvScoreOverTime)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("avgAlgorithmNotTransferredConvScoreOverTime")
          val h: ParsedDateHistogram = pf.getAggregations.get("avgAlgorithmNotTransferredConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgAlgorithmTransferredConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmTransferredConvScoreOverTime)) {
          val pf: ParsedFilter = searchResp.getAggregations.get("avgAlgorithmTransferredConvScoreOverTime")
          val h: ParsedDateHistogram = pf.getAggregations.get("avgAlgorithmTransferredConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgFeedbackConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackConvScoreOverTime)) {
          val h: ParsedDateHistogram = searchResp.getAggregations.get("avgFeedbackConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgAlgorithmAnswerScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmAnswerScoreOverTime)) {
          val h: ParsedDateHistogram = searchResp.getAggregations.get("avgAlgorithmAnswerScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgFeedbackAnswerScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackAnswerScoreOverTime)) {
          val h: ParsedDateHistogram = searchResp.getAggregations.get("avgFeedbackAnswerScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None
        val avgAlgorithmConvScoreOverTime: Option[List[AvgScoresHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmConvScoreOverTime)) {
          val h: ParsedDateHistogram = searchResp.getAggregations.get("avgAlgorithmConvScoreOverTime")
          Some {
            h.getBuckets.asScala.map { bucket =>
              val avg: Avg = bucket.getAggregations.get("avgScore")
              AvgScoresHistogramItem(
                key = bucket.getKey.asInstanceOf[ZonedDateTime].toInstant.toEpochMilli,
                keyAsString = bucket.getKeyAsString,
                docCount = bucket.getDocCount,
                avgScore = avg.getValue
              )
            }.toList
          }
        } else None

        val labelCountHistograms: Map[String, List[LabelCountHistogramItem]] = Map(
          "qaMatchedStatesHistogram" -> qaMatchedStatesHistogram,
          "qaMatchedStatesWithScoreHistogram" -> qaMatchedStatesWithScoreHistogram,
        ).filter { case (_, v) => v.nonEmpty }.map { case (k, v) => (k, v.get) }

        val scoreHistograms: Map[String, List[ScoreHistogramItem]] = Map(
          "scoreHistogram" -> scoreHistogram,
          "scoreHistogramNotTransferred" -> scoreHistogramNotTransferred,
          "scoreHistogramTransferred" -> scoreHistogramTransferred
        ).filter { case (_, v) => v.nonEmpty }.map { case (k, v) => (k, v.get) }

        val countOverTimeHistograms: Map[String, List[CountOverTimeHistogramItem]] = Map(
          "conversationsHistogram" -> conversationsHistogram,
          "conversationsNotTransferredHistogram" -> conversationsNotTransferredHistogram,
          "conversationsTransferredHistogram" -> conversationsTransferredHistogram,
          "qaPairHistogram" -> qaPairHistogram,
          "qaPairAnsweredHistogram" -> qaPairAnsweredHistogram,
          "qaPairUnansweredHistogram" -> qaPairUnansweredHistogram
        ).filter { case (_, v) => v.nonEmpty }.map { case (k, v) => (k, v.get) }

        val scoresOverTime: Map[String, List[AvgScoresHistogramItem]] = Map(
          "avgFeedbackNotTransferredConvScoreOverTime" -> avgFeedbackNotTransferredConvScoreOverTime,
          "avgFeedbackTransferredConvScoreOverTime" -> avgFeedbackTransferredConvScoreOverTime,
          "avgAlgorithmNotTransferredConvScoreOverTime" -> avgAlgorithmNotTransferredConvScoreOverTime,
          "avgAlgorithmTransferredConvScoreOverTime" -> avgAlgorithmTransferredConvScoreOverTime,
          "avgFeedbackConvScoreOverTime" -> avgFeedbackConvScoreOverTime,
          "avgAlgorithmAnswerScoreOverTime" -> avgAlgorithmAnswerScoreOverTime,
          "avgFeedbackAnswerScoreOverTime" -> avgFeedbackAnswerScoreOverTime,
          "avgAlgorithmConvScoreOverTime" -> avgAlgorithmConvScoreOverTime
        ).filter { case (_, v) => v.nonEmpty }.map { case (k, v) => (k, v.get) }

        QAAggregatedAnalytics(totalDocuments = totalDocuments.getValue,
          totalConversations = totalConversations.getValue,
          avgFeedbackConvScore = avgFeedbackConvScore,
          avgFeedbackAnswerScore = avgFeedbackAnswerScore,
          avgAlgorithmConvScore = avgAlgorithmConvScore,
          avgAlgorithmAnswerScore = avgAlgorithmAnswerScore,
          labelCountHistograms = if (labelCountHistograms.nonEmpty) Some(labelCountHistograms) else None,
          scoreHistograms = if (scoreHistograms.nonEmpty) Some(scoreHistograms) else None,
          countOverTimeHistograms = if (countOverTimeHistograms.nonEmpty) Some(countOverTimeHistograms) else None,
          scoresOverTime = if (scoresOverTime.nonEmpty) Some(scoresOverTime) else None
        )
      case _ =>
        QAAggregatedAnalytics(totalDocuments = totalDocuments.getValue,
          totalConversations = totalConversations.getValue)
    }

    res
  }

  private[this] def createAggregations(request: QAAggregatedAnalyticsRequest, firstIndexInConv: Long,
                                       dateHistInterval: DateHistogramInterval, minDocInBuckets: Long): List[AggregationBuilder] = {
    val aggregationBuilderList = new ListBuffer[AggregationBuilder]()
    aggregationBuilderList += (AggregationBuilders.cardinality("totalDocuments")
      .field("_id").precisionThreshold(40000),
      AggregationBuilders.cardinality("totalConversations")
        .field("conversation").precisionThreshold(4000))

    val dateHistTimezone = request.timezone match {
      case Some(tz) => ZoneId.ofOffset("UTC", ZoneOffset.of(tz))
      case _ => ZoneId.ofOffset("UTC", ZoneOffset.of("+00:00"))
    }

    request.aggregations match {
      case Some(aggregationsReq) =>
        val reqAggs = aggregationsReq.toSet
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackConvScore)) {
          aggregationBuilderList += AggregationBuilders
            .avg("avgFeedbackConvScore").field("feedbackConvScore")
        }
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackAnswerScore)) {
          aggregationBuilderList += AggregationBuilders
            .avg("avgFeedbackAnswerScore").field("feedbackAnswerScore")
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmConvScore)) {
          aggregationBuilderList += AggregationBuilders
            .avg("avgAlgorithmConvScore").field("algorithmConvScore")
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmAnswerScore)) {
          aggregationBuilderList += AggregationBuilders.avg("avgAlgorithmAnswerScore")
            .field("algorithmAnswerScore")
        }
        if (reqAggs.contains(QAAggregationsTypes.scoreHistogram)) {
          aggregationBuilderList += AggregationBuilders
            .histogram("scoreHistogram").field("feedbackConvScore")
            .interval(1.0d).minDocCount(minDocInBuckets)
        }
        if (reqAggs.contains(QAAggregationsTypes.scoreHistogramNotTransferred)) {
          aggregationBuilderList += AggregationBuilders.filter("scoreHistogramNotTransferred",
            QueryBuilders.boolQuery().mustNot(
              QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString)
            )
          ).subAggregation(
            AggregationBuilders
              .histogram("scoreHistogramNotTransferred").field("feedbackConvScore")
              .interval(1.0d).minDocCount(minDocInBuckets)
          )
        }
        if (reqAggs.contains(QAAggregationsTypes.scoreHistogramTransferred)) {
          aggregationBuilderList += AggregationBuilders.filter("scoreHistogramTransferred",
            QueryBuilders.boolQuery()
              .must(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
          ).subAggregation(
            AggregationBuilders
              .histogram("scoreHistogramTransferred").field("feedbackConvScore")
              .interval(1.0d).minDocCount(minDocInBuckets)
          )
        }
        if (reqAggs.contains(QAAggregationsTypes.conversationsHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("conversationsHistogram",
              QueryBuilders.termQuery("index_in_conversation", firstIndexInConv)).subAggregation(
              AggregationBuilders
                .dateHistogram("conversationsHistogram").field("timestamp")
                .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
            )
        }
        if (reqAggs.contains(QAAggregationsTypes.conversationsNotTransferredHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("conversationsNotTransferredHistogram",
              QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
                .must(QueryBuilders.termQuery("index_in_conversation", firstIndexInConv)))
              .subAggregation(
                AggregationBuilders
                  .dateHistogram("conversationsNotTransferredHistogram").field("timestamp")
                  .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                  .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.conversationsTransferredHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("conversationsTransferredHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
                .must(QueryBuilders.termQuery("index_in_conversation", firstIndexInConv)))
              .subAggregation(
                AggregationBuilders
                  .dateHistogram("conversationsTransferredHistogram").field("timestamp")
                  .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                  .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.qaPairHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("qaPairHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("doctype", Doctypes.NORMAL.toString))
                .must(QueryBuilders.termQuery("agent", Agent.STARCHAT.toString)))
              .subAggregation(
                AggregationBuilders
                  .dateHistogram("qaPairHistogram").field("timestamp")
                  .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                  .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.qaPairAnsweredHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("qaPairAnsweredHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("answered", Answered.ANSWERED.toString))
                .must(QueryBuilders.termQuery("doctype", Doctypes.NORMAL.toString))
                .must(QueryBuilders.termQuery("agent", Agent.STARCHAT.toString)))
              .subAggregation(
                AggregationBuilders
                  .dateHistogram("qaPairAnsweredHistogram").field("timestamp")
                  .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                  .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.qaPairUnansweredHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("qaPairUnansweredHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("answered", Answered.UNANSWERED.toString))
                .must(QueryBuilders.termQuery("doctype", Doctypes.NORMAL.toString))
                .must(QueryBuilders.termQuery("agent", Agent.STARCHAT.toString)))
              .subAggregation(
                AggregationBuilders
                  .dateHistogram("qaPairUnansweredHistogram").field("timestamp")
                  .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                  .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.qaMatchedStatesHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("qaMatchedStatesHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("doctype", Doctypes.NORMAL.toString))
                .must(QueryBuilders.termQuery("agent", Agent.STARCHAT.toString)))
              .subAggregation(
                AggregationBuilders.terms("qaMatchedStatesHistogram").field("state")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.qaMatchedStatesWithScoreHistogram)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("qaMatchedStatesWithScoreHistogram",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("feedbackAnswerScore").gte(0.0))
                .must(QueryBuilders.termQuery("doctype", Doctypes.NORMAL.toString))
                .must(QueryBuilders.termQuery("agent", Agent.STARCHAT.toString)))
              .subAggregation(
                AggregationBuilders.terms("qaMatchedStatesWithScoreHistogram").field("state")
              )
        }
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackNotTransferredConvScoreOverTime)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("avgFeedbackNotTransferredConvScoreOverTime",
              QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
            ).subAggregation(
              AggregationBuilders
                .dateHistogram("avgFeedbackNotTransferredConvScoreOverTime").field("timestamp")
                .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
                .subAggregation(AggregationBuilders.avg("avgScore").field("feedbackConvScore"))
            )
        }
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackTransferredConvScoreOverTime)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("avgFeedbackTransferredConvScoreOverTime",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
            ).subAggregation(
              AggregationBuilders
                .dateHistogram("avgFeedbackTransferredConvScoreOverTime").field("timestamp")
                .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
                .subAggregation(AggregationBuilders.avg("avgScore").field("feedbackConvScore"))
            )
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmNotTransferredConvScoreOverTime)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("avgAlgorithmNotTransferredConvScoreOverTime",
              QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
            ).subAggregation(
              AggregationBuilders
                .dateHistogram("avgAlgorithmNotTransferredConvScoreOverTime").field("timestamp")
                .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
                .subAggregation(AggregationBuilders.avg("avgScore").field("algorithmConvScore"))
            )
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmTransferredConvScoreOverTime)) {
          aggregationBuilderList +=
            AggregationBuilders.filter("avgAlgorithmTransferredConvScoreOverTime",
              QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("escalated", Escalated.TRANSFERRED.toString))
            ).subAggregation(
              AggregationBuilders
                .dateHistogram("avgAlgorithmTransferredConvScoreOverTime").field("timestamp")
                .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
                .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
                .subAggregation(AggregationBuilders.avg("avgScore").field("algorithmConvScore"))
            )
        }
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackConvScoreOverTime)) {
          aggregationBuilderList +=
            AggregationBuilders
              .dateHistogram("avgFeedbackConvScoreOverTime").field("timestamp")
              .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
              .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
              .subAggregation(AggregationBuilders.avg("avgScore").field("feedbackConvScore"))
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmAnswerScoreOverTime)) {
          aggregationBuilderList += AggregationBuilders
            .dateHistogram("avgAlgorithmAnswerScoreOverTime").field("timestamp")
            .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
            .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
            .subAggregation(AggregationBuilders.avg("avgScore").field("algorithmAnswerScore"))
        }
        if (reqAggs.contains(QAAggregationsTypes.avgFeedbackAnswerScoreOverTime)) {
          aggregationBuilderList += AggregationBuilders
            .dateHistogram("avgFeedbackAnswerScoreOverTime").field("timestamp")
            .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
            .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
            .subAggregation(AggregationBuilders.avg("avgScore").field("feedbackAnswerScore"))
        }
        if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmConvScoreOverTime)) {
          aggregationBuilderList += AggregationBuilders
            .dateHistogram("avgAlgorithmConvScoreOverTime").field("timestamp")
            .calendarInterval(dateHistInterval).minDocCount(minDocInBuckets)
            .timeZone(dateHistTimezone).format("yyyy-MM-dd : HH:mm:ss")
            .subAggregation(AggregationBuilders.avg("avgScore").field("algorithmConvScore"))
        }

      case _ => List.empty[QAAggregationsTypes.Value]
    }
    aggregationBuilderList.toList
  }

  def create(indexName: String, document: QADocument, refresh: Int): Option[IndexDocumentResult] = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)
    builder.field("instance", instance)
    builder.field("conversation", document.conversation)

    if (document.indexInConversation <= 0) throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
    builder.field("index_in_conversation", document.indexInConversation)

    document.status match {
      case Some(t) => builder.field("status", t)
      case _ => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case _ => builder.field("timestamp", Time.timestampMillis)
    }

    // begin core data
    document.coreData match {
      case Some(coreData) =>
        coreData.question match {
          case Some("") => ;
          case Some(t) =>
            builder.field("question", t)
          case _ => ;
        }
        coreData.questionNegative match {
          case Some(t) =>
            val array = builder.startArray("question_negative")
            t.foreach(q => {
              array.startObject().field("query", q).endObject()
            })
            array.endArray()
          case _ => ;
        }
        coreData.questionScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("question_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.answer match {
          case Some("") => ;
          case Some(t) => builder.field("answer", t)
          case _ => ;
        }
        coreData.answerScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("answer_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.topics match {
          case Some(t) => builder.field("topics", t)
          case _ => ;
        }
        coreData.verified match {
          case Some(t) => builder.field("verified", t)
          case _ => builder.field("verified", false)

        }
        coreData.done match {
          case Some(t) => builder.field("done", t)
          case _ => builder.field("done", false)
        }
      case _ => QADocumentCore()
    }
    // end core data

    // begin annotations
    document.annotations match {
      case Some(annotations) =>
        annotations.dclass match {
          case Some(t) => builder.field("dclass", t)
          case _ => ;
        }
        annotations.doctype match {
          case Some(t) => builder.field("doctype", t.toString)
          case _ => builder.field("doctype", "NORMAL");
        }
        annotations.state match {
          case Some(t) => builder.field("state", t)
          case _ => ;
        }
        annotations.agent match {
          case Some(t) => builder.field("agent", t.toString)
          case _ => builder.field("agent", "STARCHAT");
        }
        annotations.escalated match {
          case Some(t) => builder.field("escalated", t.toString)
          case _ => builder.field("escalated", "UNSPECIFIED");
        }
        annotations.answered match {
          case Some(t) => builder.field("answered", t.toString)
          case _ => builder.field("answered", "ANSWERED");
        }
        annotations.triggered match {
          case Some(t) => builder.field("triggered", t.toString)
          case _ => builder.field("triggered", "UNSPECIFIED");
        }
        annotations.followup match {
          case Some(t) => builder.field("followup", t.toString)
          case _ => builder.field("followup", "UNSPECIFIED");
        }
        annotations.feedbackConv match {
          case Some(t) => builder.field("feedbackConv", t)
          case _ => ;
        }
        annotations.feedbackConvScore match {
          case Some(t) => builder.field("feedbackConvScore", t)
          case _ => ;
        }
        annotations.algorithmConvScore match {
          case Some(t) => builder.field("algorithmConvScore", t)
          case _ => ;
        }
        annotations.feedbackAnswerScore match {
          case Some(t) => builder.field("feedbackAnswerScore", t)
          case _ => ;
        }
        annotations.algorithmAnswerScore match {
          case Some(t) => builder.field("algorithmAnswerScore", t)
          case _ => ;
        }
        annotations.responseScore match {
          case Some(t) => builder.field("responseScore", t)
          case _ => ;
        }
        annotations.start match {
          case Some(t) => builder.field("start", t)
          case _ => builder.field("start", false);
        }
      case _ => QADocumentAnnotations()
    }
    // end annotations

    builder.endObject()

    val response: IndexResponse = indexLanguageCrud.create(instance, document.id, builder)

    refreshIndex(indexName, refresh, indexLanguageCrud)

    val doc_result: IndexDocumentResult = IndexDocumentResult(index = response.getIndex,
      id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {
      doc_result
    }
  }

  private[this] def updateBuilder(document: QADocumentUpdate, instance: String): XContentBuilder = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("instance", instance)

    document.conversation match {
      case Some(t) => builder.field("conversation", t)
      case _ => ;
    }

    document.indexInConversation match {
      case Some(t) =>
        if (t <= 0) throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
        builder.field("indexInConversation", t)
      case _ => ;
    }

    document.status match {
      case Some(t) => builder.field("status", t)
      case _ => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case _ => ;
    }

    // begin core data
    document.coreData match {
      case Some(coreData) =>
        coreData.question match {
          case Some(t) => builder.field("question", t)
          case _ => ;
        }
        coreData.questionNegative match {
          case Some(t) =>
            val array = builder.startArray("question_negative")
            t.foreach(q => {
              array.startObject().field("query", q).endObject()
            })
            array.endArray()
          case _ => ;
        }
        coreData.questionScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("question_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.answer match {
          case Some(t) => builder.field("answer", t)
          case _ => ;
        }
        coreData.answerScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("answer_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.topics match {
          case Some(t) => builder.field("topics", t)
          case _ => ;
        }
        coreData.verified match {
          case Some(t) => builder.field("verified", t)
          case _ => ;
        }
        coreData.done match {
          case Some(t) => builder.field("done", t)
          case _ => ;
        }
      case _ => QADocumentCore()
    }
    // end core data

    // begin annotations
    document.annotations match {
      case Some(annotations) =>
        annotations.dclass match {
          case Some(t) => builder.field("dclass", t)
          case _ => ;
        }
        builder.field("doctype", annotations.doctype.toString)
        annotations.state match {
          case Some(t) =>
            builder.field("state", t)
          case _ => ;
        }
        annotations.agent match {
          case Some(t) =>
            builder.field("agent", t.toString)
          case _ => ;
        }
        annotations.escalated match {
          case Some(t) =>
            builder.field("escalated", t.toString)
          case _ => ;
        }
        annotations.answered match {
          case Some(t) =>
            builder.field("answered", t.toString)
          case _ => ;
        }
        annotations.triggered match {
          case Some(t) =>
            builder.field("triggered", t.toString)
          case _ => ;
        }
        annotations.followup match {
          case Some(t) =>
            builder.field("followup", t.toString)
          case _ => ;
        }
        annotations.feedbackConv match {
          case Some(t) => builder.field("feedbackConv", t)
          case _ => ;
        }
        annotations.feedbackConvScore match {
          case Some(t) => builder.field("feedbackConvScore", t)
          case _ => ;
        }
        annotations.algorithmConvScore match {
          case Some(t) => builder.field("algorithmConvScore", t)
          case _ => ;
        }
        annotations.feedbackAnswerScore match {
          case Some(t) => builder.field("feedbackAnswerScore", t)
          case _ => ;
        }
        annotations.algorithmAnswerScore match {
          case Some(t) => builder.field("algorithmAnswerScore", t)
          case _ => ;
        }
        annotations.responseScore match {
          case Some(t) => builder.field("responseScore", t)
          case _ => ;
        }
        annotations.start match {
          case Some(t) => builder.field("start", t)
          case _ => ;
        }
      case _ => ;
    }
    // end annotations

    builder.endObject()
    builder
  }

  def update(indexName: String, document: QADocumentUpdate, refresh: Int): UpdateDocumentsResult = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val builder = updateBuilder(document, instance)
    val bulkResponse = indexLanguageCrud.bulkUpdate(document.id.map(x => (x, builder)))

    refreshIndex(indexName, refresh, indexLanguageCrud)

    val listOfDocRes: List[UpdateDocumentResult] = bulkResponse.getItems.map(response => {
      UpdateDocumentResult(index = response.getIndex,
        id = response.getId,
        version = response.getVersion,
        created = response.status === RestStatus.CREATED
      )
    }).toList

    UpdateDocumentsResult(data = listOfDocRes)
  }

  private[this] def refreshIndex(indexName: String, refresh: Int, indexLanguageCrud: IndexLanguageCrud): Unit = {
    if (refresh =/= 0) {
      val refresh_index = indexLanguageCrud.refresh()
      if (refresh_index.failedShardsN > 0) {
        throw QuestionAnswerServiceException("index refresh failed: (" + indexName + ")")
      }
    }
  }

  def updateByQuery(indexName: String, updateReq: UpdateQAByQueryReq, refresh: Int): UpdateDocumentsResult = {
    val searchRes: Option[SearchQADocumentsResults] =
      search(indexName = indexName, documentSearch = updateReq.documentSearch)
    searchRes match {
      case Some(r) =>
        val id = r.hits.map(_.document.id)
        if (id.nonEmpty) {
          val updateDoc = updateReq.document.copy(id = id)
          update(indexName = indexName, document = updateDoc, refresh = refresh)
        } else {
          UpdateDocumentsResult(data = List.empty[UpdateDocumentResult])
        }
      case _ => UpdateDocumentsResult(data = List.empty[UpdateDocumentResult])
    }
  }

  def read(indexName: String, ids: List[String]): Option[SearchQADocumentsResults] = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val response = indexLanguageCrud.readAll(ids)
    val filteredDoc = response.getResponses.toList
      .filter(p => p.getResponse.isExists)
      .map { e =>
        val item = e.getResponse
        val source = item.getSource.asScala.toMap
        val document = documentFromMap(indexName, item.getId, source)
        SearchQADocument(score = .0f, document = document)
      }

    val searchResults = SearchQADocumentsResults(totalHits = filteredDoc.length, hitsCount = filteredDoc.length,
      hits = filteredDoc)

    Option {
      searchResults
    }
  }

  def allDocuments(indexName: String, keepAlive: Long = 60000, size: Int = 100): Iterator[QADocument] = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val query = QueryBuilders.matchAllQuery
    var scrollResp = indexLanguageCrud.read(instance, query, maxItems = Option(size), scroll = true, scrollTime = keepAlive)

    val scrollId = scrollResp.getScrollId

    Iterator.continually {
      val documents = scrollResp.getHits.getHits.toList.map { item =>
        val id: String = item.getId
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        documentFromMap(indexName, id, source)
      }
      scrollResp = indexLanguageCrud.scroll(new SearchScrollRequest(scrollId))
      (documents, documents.nonEmpty)
    }.takeWhile { case (_, docNonEmpty) => docNonEmpty }
      .flatMap { case (doc, _) => doc }
  }

  private[this] def extractionReq(text: String, er: UpdateQATermsRequest) = TermsExtractionRequest(text = text,
    tokenizer = Some("space_punctuation"),
    commonOrSpecificSearchPrior = Some(CommonOrSpecificSearch.COMMON),
    commonOrSpecificSearchObserved = Some(CommonOrSpecificSearch.IDXSPECIFIC),
    observedDataSource = Some(ObservedDataSources.KNOWLEDGEBASE),
    fieldsPrior = Some(TermCountFields.all),
    fieldsObserved = Some(TermCountFields.all),
    minWordsPerSentence = Some(10),
    pruneTermsThreshold = Some(100000),
    misspellMaxOccurrence = Some(5),
    activePotentialDecay = Some(10),
    activePotential = Some(true),
    totalInfo = Some(false))

  def updateTextTerms(indexName: String, extractionRequest: UpdateQATermsRequest): List[UpdateDocumentResult] = {
    val ids: List[String] = List(extractionRequest.id)
    val q = this.read(indexName, ids)
    val documentResults = q.getOrElse(SearchQADocumentsResults())
    documentResults.hits.filter(_.document.coreData.nonEmpty).map { hit =>
      hit.document.coreData match {
        case Some(coreData) =>
          val extractionReqQ = extractionReq(text = coreData.question.getOrElse(""), er = extractionRequest)
          val (_, termsQ) = manausTermsExtractionService
            .textTerms(indexName = indexName, extractionRequest = extractionReqQ)
          val extractionReqA = extractionReq(text = coreData.answer.getOrElse(""), er = extractionRequest)
          val (_, termsA) = manausTermsExtractionService
            .textTerms(indexName = indexName, extractionRequest = extractionReqA)
          val scoredTermsUpdateReq = QADocumentUpdate(
            id = ids,
            coreData = Some(
              QADocumentCore(
                questionScoredTerms = Some(termsQ.toList),
                answerScoredTerms = Some(termsA.toList)
              )
            )
          )
          val res = update(indexName = indexName, document = scoredTermsUpdateReq, refresh = 0)
          res.data.headOption.getOrElse(
            UpdateDocumentResult(index = indexName, id = hit.document.id, version = -1, created = false)
          )
        case _ =>
          UpdateDocumentResult(index = indexName, id = hit.document.id, version = -1, created = false)
      }
    }
  }

  def updateAllTextTerms(indexName: String,
                         extractionRequest: UpdateQATermsRequest,
                         keepAlive: Long = 3600000): Iterator[UpdateDocumentResult] = {
    allDocuments(indexName = indexName, keepAlive = keepAlive).filter(_.coreData.nonEmpty).map { item =>
      item.coreData match {
        case Some(coreData) =>
          val extractionReqQ = extractionReq(text = coreData.question.getOrElse(""), er = extractionRequest)
          val extractionReqA = extractionReq(text = coreData.answer.getOrElse(""), er = extractionRequest)
          val (_, termsQ) = manausTermsExtractionService
            .textTerms(indexName = indexName, extractionRequest = extractionReqQ)
          val (_, termsA) = manausTermsExtractionService
            .textTerms(indexName = indexName, extractionRequest = extractionReqA)

          val scoredTermsUpdateReq = QADocumentUpdate(
            id = List[String](item.id),
            coreData = Some(
              QADocumentCore(
                questionScoredTerms = Some(termsQ.toList),
                answerScoredTerms = Some(termsA.toList)
              )
            )
          )

          val res = update(indexName = indexName, document = scoredTermsUpdateReq, refresh = 0)
          res.data.headOption.getOrElse(
            UpdateDocumentResult(index = indexName, id = item.id, version = -1, created = false)
          )
        case _ =>
          UpdateDocumentResult(index = indexName, id = item.id, version = -1, created = false)
      }
    }
  }

  override def delete(indexName: String, ids: List[String], refresh: Int): DeleteDocumentsResult = {
    val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(indexName, elasticClient.indexSuffix)
    super.delete(esLanguageSpecificIndexName, ids, refresh)
  }

  override def deleteAll(indexName: String): DeleteDocumentsSummaryResult = {
    val instance = Index.instanceName(indexName)
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val response = indexLanguageCrud.delete(instance, QueryBuilders.matchAllQuery)

    DeleteDocumentsSummaryResult(message = "delete", deleted = response.getTotal)
  }
}
