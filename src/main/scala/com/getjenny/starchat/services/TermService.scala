package com.getjenny.starchat.services

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 10/03/17.
 */

import java.io.File
import java.net.URL

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.analyzer.util.VectorUtils
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.analyzer.utils.TextToVectorsTools
import com.getjenny.starchat.entities._
import com.getjenny.starchat.entities.es._
import com.getjenny.starchat.services.esclient.TermElasticClient
import com.getjenny.starchat.services.esclient.crud.IndexLanguageCrud
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.indices.{AnalyzeRequest, AnalyzeResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.List

case class TermServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
 * Implements functions, eventually used by TermResource
 */
object TermService extends AbstractDataService {
  override val elasticClient: TermElasticClient.type = TermElasticClient
  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)
  val defaultOrg: String = TermElasticClient.commonIndexDefaultOrgPattern

  /** fetch the index arbitrary pattern
   *
   * @return the index arbitrary pattern
   */
  def commonIndexArbitraryPattern: String = elasticClient.commonIndexArbitraryPattern

  /** Populate synonyms from resource file (a default synonyms list)
   *
   * @param indexName name of the index
   * @param refresh whether to call an index update on ElasticSearch or not
   * @return a return message with the number of successfully and failed indexing operations
   */
  def indexDefaultSynonyms(indexName: String, refresh: Int = 0): UpdateDocumentsResult = {
    val (_, language, _) = Index.patternsFromIndexName(indexName)
    val synonymsPath: String = "/index_management/json_index_spec/" + language + "/synonyms.csv"
    val synonymsResource: URL = getClass.getResource(synonymsPath)
    val synFile = new File(synonymsResource.toString.replaceFirst("file:", ""))
    this.indexSynonymsFromCsvFile(indexName = indexName, file = synFile)
  }

  /** upload a file with Synonyms, it replace existing terms but does not remove synonyms for terms not in file.
   *
   * @param indexName the index name
   * @param file a File object with the synonyms
   * @param skipLines how many lines to skip
   * @param separator a separator, usually the comma character
   * @return the IndexDocumentListResult with the indexing result
   */
  def indexSynonymsFromCsvFile(indexName: String, file: File, skipLines: Int = 0, separator: Char = ','): UpdateDocumentsResult = {
    val documents = FileToDocuments.getTermsDocumentsFromCSV(log = log,
      file = file, skipLines = skipLines, separator = separator).toList
    updateTerm(indexName, Terms(terms = documents), 0)
  }

  /** index terms on Elasticsearch
   *
   * @param indexName the index name
   * @param terms the terms
   * @param refresh whether to call an index update on ElasticSearch or not
   * @return list of indexing responses
   */
  def indexTerm(indexName: String, terms: Terms, refresh: Int): IndexDocumentListResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val inputTerms = terms.terms.map(x => x.term -> x)

    val listOfDocRes = indexLanguageCrud.bulkCreate(inputTerms, new TermEntityManager)
    IndexDocumentListResult(listOfDocRes)
  }

  /** fetch one or more terms from Elasticsearch
   *
   * @param indexName the index name
   * @param termsRequest the ids of the terms to be fetched
   * @return fetched terms
   */
  def termsById(indexName: String, termsRequest: DocsIds): Terms = {
    if (termsRequest.ids.nonEmpty) {
      val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
      val terms = indexLanguageCrud
        .readAll(termsRequest.ids, new TermEntityManager)
        .headOption.map(_.terms).getOrElse(List.empty)
      Terms(terms)
    } else {
      Terms(terms = List.empty)
    }
  }

  /** update terms, synchronous function
   *
   * @param indexName index name
   * @param terms terms to update
   * @param refresh whether to call an index update on ElasticSearch or not
   * @return result of the update operations
   */
  def updateTerm(indexName: String, terms: Terms, refresh: Int): UpdateDocumentsResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val inputTerms = terms.terms.map(x => x.term -> x)

    val listOfDocRes = indexLanguageCrud.bulkUpdate(inputTerms, upsert = true, new TermEntityManager, refresh)


    UpdateDocumentsResult(listOfDocRes)
  }

  /** fetch two terms and calculate the distance between them
   *
   * @param indexName the index name
   * @param termsReq list of terms
   * @return the distance between terms
   */
  def termsDistance(indexName: String, termsReq: DocsIds): List[TermsDistanceRes] = {
    val extractedTerms = termsById(indexName, DocsIds(ids = termsReq.ids))
    val retrievedTerms = extractedTerms.terms.map { t => (t.term, t) }.toMap

    retrievedTerms
      .keys.flatMap(a => retrievedTerms.keys.map(b => (a, b)))
      .filter { case (t1, t2) => t1 =/= t2 }.map { case (t1, t2) =>
      val v1 = retrievedTerms(t1).vector.getOrElse(TextToVectorsTools.emptyVec())
      val v2 = retrievedTerms(t2).vector.getOrElse(TextToVectorsTools.emptyVec())
      TermsDistanceRes(
        term1 = t1,
        term2 = t2,
        vector1 = v1,
        vector2 = v2,
        cosDistance = VectorUtils.cosineDist(v1, v2),
        eucDistance = VectorUtils.euclideanDist(v1, v2)
      )
    }.toList
  }

  class StringOrSearchTerm[T]

  object StringOrSearchTerm {

    implicit object SearchTermWitness extends StringOrSearchTerm[SearchTerm]

    implicit object StringWitness extends StringOrSearchTerm[String]

  }

  /** given a text, return all the matching terms
   *
   * @param indexName index term
   * @param query input text or SearchTerm entity
   * @param analyzer the analyzer name to be used for text tokenization
   * @return the terms found
   */
  def search[T: StringOrSearchTerm](indexName: String, query: T,
                                    analyzer: String = "space_punctuation"): TermsResults = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val term_field_name = if (TokenizersDescription.analyzersMap.contains(analyzer)) {
      "term." + analyzer
    } else {
      throw TermServiceException("search: analyzer not found or not supported: (" + analyzer + ")")
    }

    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()

    query match {
      case term: SearchTerm =>
        term.term.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery(term_field_name, x)))
        term.frequencyBase.foreach(x => QueryBuilders.termQuery("frequency_base", x))
        term.frequencyStem.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery("frequency_stem", x)))
        term.synonyms.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery("synonyms", x)))
        term.antonyms.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery("antonyms", x)))
        term.tags.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery("tags", x)))
        term.features.foreach(x => boolQueryBuilder.must(QueryBuilders.termQuery("features", x)))
      case text: String =>
        boolQueryBuilder.should(QueryBuilders.matchQuery(term_field_name, text))
      case _ =>
        throw TermServiceException("Unexpected query type for terms search")
    }

    val termsInfo: Option[TermsInfo] = indexLanguageCrud.read(boolQueryBuilder,
      searchType = SearchType.DFS_QUERY_THEN_FETCH,
      version = Option(true),
      entityManager = new TermEntityManager).headOption

    val terms = termsInfo.map(_.terms).getOrElse(List.empty)
    val maxScore = termsInfo.flatMap(_.maxScore).getOrElse(0f)
    TermsResults(total = terms.length, maxScore = maxScore, hits = Terms(terms))
  }

  /** tokenize a text
   *
   * @param indexName index name
   * @param query a TokenizerQueryRequest with the text to tokenize
   * @return a TokenizerResponse with the result of the tokenization
   */
  def esTokenizer(indexName: String, query: TokenizerQueryRequest): TokenizerResponse = {
    val esLangIndexName = Index.esLanguageFromIndexName(indexName, elasticClient.indexSuffix)
    val analyzer = TokenizersDescription.analyzersMap.get(query.tokenizer) match {
      case Some((analyzerEsName, _)) => analyzerEsName
      case _ =>
        throw TermServiceException("esTokenizer: analyzer not found or not supported: (" + query.tokenizer + ")")
    }

    val client: RestHighLevelClient = elasticClient.httpClient

    val analyzerReq = AnalyzeRequest.withIndexAnalyzer(
      esLangIndexName,
      analyzer,
      query.text
    )

    val analyzeResponse: AnalyzeResponse = client.indices().analyze(analyzerReq, RequestOptions.DEFAULT)

    val tokenizationRes: List[AnalyzeResponse.AnalyzeToken] = if (analyzeResponse.getTokens != null)
      analyzeResponse.getTokens.listIterator.asScala.toList
    else
      List.empty[AnalyzeResponse.AnalyzeToken]

    val tokens: List[TokenizerResponseItem] =
      tokenizationRes.map(t => {
        val responseItem: TokenizerResponseItem =
          TokenizerResponseItem(startOffset = t.getStartOffset,
            position = t.getPosition,
            endOffset = t.getEndOffset,
            token = t.getTerm,
            tokenType = t.getType)
        responseItem
      })

    TokenizerResponse(tokens = tokens)
  }

  /** tokenize a sentence and extract term vectors for each token
   *
   * @param indexName index name
   * @param text input text
   * @param analyzer analyzer name
   * @param unique if true exclude from results duplicated terms
   * @return the TextTerms
   */
  def textToVectors(indexName: String, text: String, analyzer: String = "stop",
                    unique: Boolean = false): TextTerms = {
    val analyzerRequest = TokenizerQueryRequest(tokenizer = analyzer, text = text) // analyzer is checked by esTokenizer
    val fullTokenList = esTokenizer(indexName, analyzerRequest).tokens
      .map(e => e.token)

    val tokenList = if (unique) fullTokenList.toSet.toList else fullTokenList
    val termsRequest = DocsIds(ids = tokenList)
    val termList = termsById(indexName, termsRequest)

    val textTerms = TextTerms(text = text,
      textTermsN = tokenList.length,
      termsFoundN = termList.terms.length,
      terms = termList
    )
    textTerms
  }

  /** fetch all documents and serve them through an iterator
   *
   * @param indexName index name
   * @param keepAlive the keep alive timeout for the ElasticSearch document scroller
   * @return an iterator for Items
   */
  def allDocuments(indexName: String, keepAlive: Long = 60000): Iterator[Term] = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val query = QueryBuilders.matchAllQuery

    indexLanguageCrud
      .scroll(query, scrollTime = keepAlive, maxItems = Option(100),
        entityManager = new TermEntityManager(false))
      .flatMap(_.terms)
  }

  override def delete(indexName: String, ids: List[String], refresh: Int): DeleteDocumentsResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)

    val response = indexLanguageCrud.delete(ids, refresh, new TermEntityManager)

    DeleteDocumentsResult(data = response)
  }

  override def deleteAll(indexName: String): DeleteDocumentsSummaryResult = {
    val indexLanguageCrud = IndexLanguageCrud(elasticClient, indexName)
    val response = indexLanguageCrud.delete(QueryBuilders.matchAllQuery)

    DeleteDocumentsSummaryResult(message = "delete", deleted = response.getTotal)
  }
}