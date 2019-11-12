package com.getjenny.starchat.services

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

import java.io.File
import java.util

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.analyzer.utils.TokenToVector
import com.getjenny.starchat.entities.es._
import com.getjenny.starchat.entities.{SearchAlgorithm, _}
import com.getjenny.starchat.services.esclient.DecisionTableElasticClient
import com.getjenny.starchat.services.esclient.crud.IndexLanguageCrud
import com.getjenny.starchat.utils.Index
import org.apache.lucene.search.join._
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.{BoolQueryBuilder, InnerHitBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.script.Script
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.AggregationBuilders
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

case class DecisionTableServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
 * Implements functions, eventually used by DecisionTableResource, for searching, get next response etc
 */
object DecisionTableService extends AbstractDataService {
  override val elasticClient: DecisionTableElasticClient.type = DecisionTableElasticClient
  private[this] val termService: TermService.type = TermService
  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)

  private[this] val queriesScoreMode: Map[String, ScoreMode] =
    Map[String, ScoreMode]("min" -> ScoreMode.Min,
      "max" -> ScoreMode.Max, "avg" -> ScoreMode.Avg, "total" -> ScoreMode.Total)

  def handleNgrams(indexName: String, analyzer: String, queries: String,
                   dtDocuments: List[SearchDTDocumentsAndNgrams]): List[SearchDTDocument] = {
    val tokenizerRequest = TokenizerQueryRequest(tokenizer = analyzer, text = queries)
    val tokenizerResponse = termService.esTokenizer(indexName, tokenizerRequest)
    val queryTokens = tokenizerResponse.tokens.map(_.token)

    val ngramsIndex = (dtDocuments.flatMap { case SearchDTDocumentsAndNgrams(_, ngrams) => ngrams }
      .flatten ++ queryTokens).distinct.zipWithIndex.toMap

    val queryVector = TokenToVector.tokensToVector(queryTokens, ngramsIndex)

    dtDocuments.map { case SearchDTDocumentsAndNgrams(searchDocument, queriesNgrams) =>
      val score = queriesNgrams.map(ngrams => {
        1.0 - TokenToVector.cosineDist(queryVector, TokenToVector.tokensToVector(ngrams, ngramsIndex))
      }).max
      (searchDocument, score)
    }.map { case (searchDtDocument, score) =>
      val document: DTDocumentCreate = searchDtDocument.document
      SearchDTDocument(score = score.toFloat, document = document)
    }
  }

  private[this] def documentSearchQueries(indexName: String,
                                          value: String,
                                          minScore: Float,
                                          boostExactMatchFactor: Float,
                                          documentSearch: DTDocumentSearch
                                         ): (QueryBuilder, Option[(String, SearchAlgorithm.Value)]) = {
    val searchAlgorithm = documentSearch.searchAlgorithm.getOrElse(SearchAlgorithm.DEFAULT)
    searchAlgorithm match {
      case SearchAlgorithm.AUTO | SearchAlgorithm.DEFAULT =>
        val (scriptBody, matchQueryEs, analyzer, algorithm) = if (documentSearch.queries.getOrElse("").length > 3) {
          (
            "return doc['queries.query.ngram_3'] ;",
            "queries.query.ngram_3",
            "ngram3",
            SearchAlgorithm.NGRAM3

          )
        } else {
          (
            "return doc['queries.query.ngram_2'] ;",
            "queries.query.ngram_2",
            "ngram2",
            SearchAlgorithm.NGRAM2
          )
        }
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery(matchQueryEs, value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option(analyzer -> algorithm))
      case SearchAlgorithm.STEM_BOOST_EXACT =>
        (
          QueryBuilders.nestedQuery(
            "queries",
            QueryBuilders.boolQuery()
              .must(QueryBuilders.matchQuery("queries.query.stem", value))
              .should(QueryBuilders.matchPhraseQuery("queries.query.raw", value)
                .boost(1 + (minScore * boostExactMatchFactor))
              ),
            queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
          ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.SHINGLES2 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.shingles_2", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.SHINGLES3 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.shingles_3", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.SHINGLES4 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.shingles_4", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.STEM_SHINGLES2 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_shingles_2", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.STEM_SHINGLES3 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_shingles_3", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.STEM_SHINGLES4 =>
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_shingles_4", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true).innerHit(new InnerHitBuilder().setSize(100)),
          None)
      case SearchAlgorithm.NGRAM2 =>
        val scriptBody = "return doc['queries.query.ngram_2'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.ngram_2", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("ngram2" -> searchAlgorithm))
      case SearchAlgorithm.STEM_NGRAM2 =>
        val scriptBody = "return doc['queries.query.stemmed_ngram_2'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_ngram_2", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("stemmed_ngram2" -> searchAlgorithm))
      case SearchAlgorithm.NGRAM3 =>
        val scriptBody = "return doc['queries.query.ngram_3'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.ngram_3", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("ngram3" -> searchAlgorithm))
      case SearchAlgorithm.STEM_NGRAM3 =>
        val scriptBody = "return doc['queries.query.stemmed_ngram_3'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_ngram_3", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("stemmed_ngram3", searchAlgorithm))
      case SearchAlgorithm.NGRAM4 =>
        val scriptBody = "return doc['queries.query.ngram_4'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.ngram_4", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("ngram4" -> searchAlgorithm))
      case SearchAlgorithm.STEM_NGRAM4 =>
        val scriptBody = "return doc['queries.query.stemmed_ngram_4'] ;"
        val script: Script = new Script(scriptBody)
        (QueryBuilders.nestedQuery(
          "queries",
          QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("queries.query.stemmed_ngram_4", value)),
          queriesScoreMode.getOrElse(elasticClient.queriesScoreMode, ScoreMode.Max)
        ).ignoreUnmapped(true)
          .innerHit(new InnerHitBuilder().addScriptField("terms", script).setSize(100)),
          Option("stemmed_ngram4" -> searchAlgorithm))
      case _ => throw DecisionTableServiceException("Unknown SearchAlgorithm: " + searchAlgorithm)
    }

  }

  def search(indexName: String, documentSearch: DTDocumentSearch): SearchDTDocumentsResults = {

    val minScore = documentSearch.minScore.getOrElse(
      Option {
        elasticClient.queryMinThreshold
      }.getOrElse(0.0f)
    )

    val boostExactMatchFactor = documentSearch.boostExactMatchFactor.getOrElse(
      Option {
        elasticClient.boostExactMatchFactor
      }.getOrElse(1.0f)
    )

    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    documentSearch.state match {
      case Some(value) => boolQueryBuilder.must(QueryBuilders.termQuery("state", value))
      case _ => ;
    }

    documentSearch.executionOrder match {
      case Some(value) =>
        boolQueryBuilder.must(QueryBuilders.matchQuery("execution_order", value))
      case _ => ;
    }

    val (queryBuilder, isNgram) = documentSearch.queries.map(x => {
      val (q, ngramsAlgorithm) = documentSearchQueries(indexName, x, minScore, boostExactMatchFactor, documentSearch)
      boolQueryBuilder.must(q) -> ngramsAlgorithm
    }).getOrElse(boolQueryBuilder -> None)

    val queryExtractorFunction = isNgram.map { case (_, alg) =>
      val sliding = alg match {
        case SearchAlgorithm.STEM_NGRAM2 | SearchAlgorithm.NGRAM2 => 2
        case SearchAlgorithm.STEM_NGRAM3 | SearchAlgorithm.NGRAM3 => 3
        case SearchAlgorithm.STEM_NGRAM4 | SearchAlgorithm.NGRAM4 => 4
        case _ => 2
      }
      queryAndNgramExtractor(sliding)
    }.getOrElse(orderedQueryExtractor)

    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val documents = indexLanguageCrud.read(queryBuilder, version = Option(true),
      from = documentSearch.from.orElse(Option(0)),
      maxItems = documentSearch.size.orElse(Option(10)),
      entityManager = new SearchDTDocumentEntityManager(queryExtractorFunction))

    val res = isNgram.map { case (analyzer, _) => handleNgrams(indexName, analyzer,
      documentSearch.queries.getOrElse(""), documents)
    }
      .getOrElse(documents.map(_.documents))

    val maxScore: Float = if (res.nonEmpty) {
      res.maxBy(_.score).score
    } else {
      0.0f
    }

    val total: Int = res.length
    SearchDTDocumentsResults(total = total, maxScore = maxScore, hits = res)
  }

  def searchDtQueries(indexName: String,
                      analyzerEvaluateRequest: AnalyzerEvaluateRequest): SearchDTDocumentsResults = {
    val dtDocumentSearch: DTDocumentSearch =
      DTDocumentSearch(
        from = Option {
          0
        },
        size = Option {
          10000
        },
        minScore = Option {
          elasticClient.queryMinThreshold
        },
        executionOrder = None: Option[Int],
        boostExactMatchFactor = Option {
          elasticClient.boostExactMatchFactor
        },
        state = None: Option[String],
        queries = Option {
          analyzerEvaluateRequest.query
        },
        searchAlgorithm = analyzerEvaluateRequest.searchAlgorithm,
        evaluationClass = analyzerEvaluateRequest.evaluationClass
      )

    this.search(indexName, dtDocumentSearch)
  }

  def resultsToMap(results: SearchDTDocumentsResults): Map[String, Any] = {
    Map("dt_queries_search_result" ->
      results.hits.map {
        doc => (doc.document.state, (doc.score, doc))
      }.toMap
    )
  }

  def create(indexName: String, document: DTDocumentCreate, refresh: Int): IndexDocumentResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val response = indexLanguageCrud.update(document, upsert = true,
      new SearchDTDocumentEntityManager(simpleQueryExtractor))

    if (refresh =/= 0) {
      val refreshIndex = indexLanguageCrud.refresh()
      if (refreshIndex.failedShardsN > 0) {
        throw new Exception("DecisionTable : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(index = response.index,
      id = response.id,
      version = response.version,
      created = response.created
    )

    docResult
  }

  def update(indexName: String, document: DTDocumentUpdate, refresh: Int): UpdateDocumentResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val response = indexLanguageCrud.update(document, entityManager = new SearchDTDocumentEntityManager(simpleQueryExtractor))

    if (refresh =/= 0) {
      val refreshIndex = indexLanguageCrud.refresh()
      if (refreshIndex.failedShardsN > 0) {
        throw new Exception("DecisionTable : index refresh failed: (" + indexName + ")")
      }
    }

    response
  }

  def getDTDocuments(indexName: String): SearchDTDocumentsResults = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val query = QueryBuilders.matchAllQuery

    //get a map of stateId -> AnalyzerItem (only if there is smt in the field "analyzer")
    val decisionTableContent = indexLanguageCrud.read(query, version = Option(true), maxItems = Option(10000),
      entityManager = new SearchDTDocumentEntityManager(simpleQueryExtractor))
      .map(_.documents)
      .sortBy(_.document.state)

    val maxScore: Float = .0f
    val total: Int = decisionTableContent.length
    SearchDTDocumentsResults(total = total, maxScore = maxScore, hits = decisionTableContent)
  }

  def read(indexName: String, ids: List[String]): SearchDTDocumentsResults = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val documents = indexLanguageCrud
      .readAll(ids, new SearchDTDocumentEntityManager(simpleQueryExtractor))
      .map(_.documents)

    val maxScore: Float = .0f
    val total: Int = documents.length
    SearchDTDocumentsResults(total = total, maxScore = maxScore, hits = documents)
  }

  private[this] val simpleQueryExtractor: (Map[String, SearchHits], Map[String, Any]) => (List[String], List[List[String]]) = (_: Map[String, SearchHits], source: Map[String, Any]) => {
    source.get("queries") match {
      case Some(t) => (t.asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
        .asScala.map { res => Some(res.getOrDefault("query", None.orNull)) }
        .filter(_.nonEmpty).map(_.get).toList, List.empty[List[String]])
      case None => (List.empty, List.empty[List[String]])
    }
  }

  private[this] val orderedQueryExtractor = (innerHits: Map[String, SearchHits], source: Map[String, Any]) => {
    source.get("queries") match {
      case Some(t) =>
        val offsets = innerHits("queries").getHits.toList.map(innerHit => {
          innerHit.getNestedIdentity.getOffset
        })
        val query_array = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]].asScala.toList
          .map(q_e => q_e.get("query"))
        (offsets.map(i => query_array(i)), List.empty[List[String]])
      case None => (List.empty, List.empty[List[String]])
    }
  }

  private[this] val queryAndNgramExtractor = (sliding: Int) => (innerHits: Map[String, SearchHits], source: Map[String, Any]) => {
    source.get("queries") match {
      case Some(t) =>
        val offsetsAndNgrams = innerHits("queries").getHits.toList.map(innerHit => {
          innerHit.getNestedIdentity.getOffset
        })
        val queryArray = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]].asScala.toList
          .map(q_e => q_e.get("query"))
        offsetsAndNgrams.map { e =>
          val qNgrams = queryArray(e).toLowerCase().replaceAll("\\s", "").sliding(sliding).toList
          (queryArray(e), qNgrams)
        }.unzip
      case None => (List.empty, List.empty[List[String]])
    }
  }

  def indexCSVFileIntoDecisionTable(indexName: String, file: File, skipLines: Int = 1, separator: Char = ','):
  IndexDocumentListResult = {
    val documents: IndexedSeq[DTDocumentCreate] = FileToDocuments.getDTDocumentsFromCSV(log = log, file = file,
      skipLines = skipLines, separator = separator)

    val indexDocumentListResult = documents.map(dtDocument => {
      create(indexName, dtDocument, 0)
    }).toList

    IndexDocumentListResult(data = indexDocumentListResult)
  }

  def indexJSONFileIntoDecisionTable(indexName: String, file: File): IndexDocumentListResult = {
    val documents: IndexedSeq[DTDocumentCreate] = FileToDocuments.getDTDocumentsFromJSON(log = log, file = file)

    val indexDocumentListResult = documents.map(dtDocument => {
      create(indexName, dtDocument, 0)
    }).toList

    IndexDocumentListResult(data = indexDocumentListResult)
  }


  def wordFrequenciesInQueries(indexName: String): Map[String, Double] = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    /*   Query to fetch for word freq in queries
    {
       "size": 0,
       "aggregations" : {
           "wordInQueries": {
               "nested": { "path": "queries" },
               "aggregations": {
                   "queries_children": {
                       "terms" : { "field": "queries.query.base" }
                   }
               }

           }
        }
    } */

    val aggregation = AggregationBuilders.nested("queries", "queries")
      .subAggregation(
        AggregationBuilders.terms("queries_children").field("queries.query.base").minDocCount(1)
      )

    val query = QueryBuilders.matchAllQuery

    val response = indexLanguageCrud.read(query,
      searchType = SearchType.DFS_QUERY_THEN_FETCH,
      requestCache = Some(true),
      maxItems = Option(0),
      minScore = Option(0.0f),
      aggregation = List(aggregation),
      entityManager = WordFrequenciesInQueryEntityManager)

    response.headOption.map(_.frequencies).getOrElse(Map.empty)
  }

  def wordFrequenciesInQueriesByState(indexName: String): List[DTStateWordFreqsItem] = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    /*   Query to fetch for each state word freq in queries
    {
       "size": 0,
       "query": {
          "bool": {
            "must": [
              {
                "nested": {
                  "path": "queries",
                  "query": {
                    "bool": {
                      "filter": {
                        "exists": {
                          "field": "queries"
                        }
                      }
                    }
                  }
                }
              }
            ]
          }
        },
        "aggs": {
        "states": {
          "terms": {
            "field": "state",
            "size": 10000000
          },
          "aggs": {
            "queries": {
              "nested": { "path": "queries" },
              "aggregations": {
                "queries_children": {
                  "terms" : { "field": "queries.query.base" }
                }
              }
            }
          }
        }
      }
    } */

    val stateAggsName = "StatesWordStats"

    // Filter all states with queries
    val query = QueryBuilders.boolQuery().must(
      QueryBuilders.nestedQuery("queries",
        QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("queries")), ScoreMode.None))

    // Calculate for each state with queries the words freq histogram.
    val aggregation = AggregationBuilders.terms(stateAggsName).field("state").size(65536).minDocCount(1)
      .subAggregation(
        AggregationBuilders.nested("queries", "queries")
          .subAggregation(
            AggregationBuilders.terms("queries_children").field("queries.query.base").minDocCount(1)
          )
      )

    indexLanguageCrud.read(query, searchType = SearchType.DFS_QUERY_THEN_FETCH,
      aggregation = List(aggregation),
      requestCache = Some(true),
      maxItems = Option(0),
      minScore = Option(0.0f),
      entityManager = DtStateWordFreqsEntityManager)
  }


  def totalQueriesMatchingRegEx(indexName: String, rx: String): Long = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    /*   Query to fetch inner hits for the queries which satisfy regex expression
    {
      "size": 10000,
      "query": {
        "nested": {
          "path": "queries",
          "query": {
            "regexp": {
              "queries.query.base": {
                "value": "acc.*",
                "flags": "ALL",
                "max_determinized_states": 10000,
                "rewrite": "constant_score"
              }
            }
          },
          "inner_hits": {
            "size": 100
          }
        }
      }
    }
    */

    val query: BoolQueryBuilder = QueryBuilders.boolQuery().should(
      QueryBuilders.nestedQuery("queries", QueryBuilders
        .regexpQuery("queries.query.base", rx)
        .maxDeterminizedStates(10000), ScoreMode.None)
        .innerHit(new InnerHitBuilder().setSize(100)) // Increase in Index Definition
    )

    val totalHits = indexLanguageCrud.read(query, searchType = SearchType.DFS_QUERY_THEN_FETCH,
      requestCache = Some(true),
      maxItems = Option(10000),
      minScore = Option(0.0f),
      entityManager = TotalQueriesMatchingEntityManager)

    totalHits.map(_.queriesTotalHits).sum
  }


  def totalQueriesOfStateMatchingRegEx(indexName: String, rx: String, stateName: String): Long = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    /*   Query to fetch inner hits for the queries which satisfy regex expression
    "size": 10000,
      "query":
      {
         "bool" : {
            "must": {
              "match": {
                "state": "quickstart"
                }
             },
            "filter": {
              "nested": {
                "path": "queries",
                "query": {
                  "regexp": {
                    "queries.query.base": {
                      "value": "start.*",
                      "flags": "ALL",
                      "max_determinized_states": 10000,
                      "rewrite": "constant_score"
                    }
                  }
                },
                  "inner_hits": {"size": 100}
              }
            }
      }
      }
      }
    */

    val query = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("state", stateName))
      .filter(QueryBuilders.nestedQuery("queries",
        QueryBuilders.regexpQuery("queries.query.base", rx).maxDeterminizedStates(10000), ScoreMode.None)
        .innerHit(new InnerHitBuilder().setSize(100))) // Increase in Index Definition

    val totalHits = indexLanguageCrud.read(query, searchType = SearchType.DFS_QUERY_THEN_FETCH,
      requestCache = Some(true),
      maxItems = Option(10000),
      minScore = Option(0.0f),
      entityManager = TotalQueriesMatchingEntityManager)

    totalHits.map(_.queriesTotalHits).sum

  }

  override def delete(indexName: String, ids: List[String], refresh: Int): DeleteDocumentsResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val response = indexLanguageCrud.delete(ids, new WriteEntityManager[DTDocument] {
      override protected def toXContentBuilder(entity: DTDocument, instance: String): (String, XContentBuilder) = ???
    })

    if (refresh =/= 0) {
      val refreshIndex = indexLanguageCrud.refresh()
      if(refreshIndex.failedShardsN > 0) {
        throw DeleteDataServiceException("index refresh failed: (" + indexName + ")")
      }
    }

    DeleteDocumentsResult(data = response)
  }


  override def deleteAll(indexName: String): DeleteDocumentsSummaryResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val response = indexLanguageCrud.delete(QueryBuilders.matchAllQuery)

    DeleteDocumentsSummaryResult(message = "delete", deleted = response.getTotal)
  }

}