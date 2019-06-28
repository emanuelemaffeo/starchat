package com.getjenny.starchat.analyzer.atoms

/**
  * Created by mal on 20/02/2017.
  */

import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import com.getjenny.starchat.entities._
import com.getjenny.starchat.services._

/**
  * Query ElasticSearch
  */
class KeywordAtomic2(arguments: List[String], restrictedArgs: Map[String, String]) extends AbstractAtomic {

  val keyword: String = arguments.headOption match {
    case Some(t) => t
    case _ =>
      throw ExceptionAtomic("search requires argument keyword")
  }

  override def toString: String = "keyword(\"" + keyword + "\")"
  val isEvaluateNormalized: Boolean = false
  override val matchThreshold: Double = 0.15 // TODO chose the right value

  val decisionTableService: DecisionTableService.type = DecisionTableService

  val conversationLogsService: ConversationLogsService.type  = ConversationLogsService

  //qaMatchedStatesWithScoreHistogram (con feedback Positivo o negativo)
  //qaMatchedStatesHistogram senza score
  // interval 1M un mese

  def evaluate(query: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {

    var P_K = 0.0d
    // First check if keyword is present in query
    if(query.contains(keyword)) {
      val currentState = data.context.stateName
      /* First step is the evaluation of P(S) = Probability that a state is triggered
       This result is achieved asking ES the occurences of state in all conversation logs
     */

      val request = QAAggregatedAnalyticsRequest(
        interval = Some("1M"),
        minDocInBuckets = Some(0),
        timestampGte = None,
        timestampLte = None,
        aggregations = Some(List(QAAggregationsTypes.qaMatchedStatesHistogram, QAAggregationsTypes.qaMatchedStatesWithScoreHistogram)),
        timezone = None)

      val queryResult: QAAggregatedAnalytics = ConversationLogsService.analytics("index_getjenny_english_0", request)

      // extract histograms qaMatchedStatesHistogram and qaMatchedStatesWithScoreHistogram
      val histograms: Map[String, List[LabelCountHistogramItem]] = queryResult.labelCountHistograms match {
        case Some(t) => t
        case _ => throw ExceptionAtomic("keyword2 atom analytics does not return histograms")
      }

      // extract histogram qaMatchedStatesHistogram (occurrences of state regardless score)
      val stateFrequenciesHist: List[LabelCountHistogramItem] = histograms.get("qaMatchedStatesHistogram") match {
        case Some(t) => t
        case _ => throw ExceptionAtomic("keyword2 atom analytics does not contain qaMatchedStatesHistogram")
      }

      // look if actually processed state is present in the histogram. There should be not more than one
      val histStateEntries = stateFrequenciesHist.filter(elem => elem.key == currentState)
      val P_S: Double = histStateEntries.length match {
        case 0 => 1.0d  // TO DO NORMALIZE ON STATE NUMBER
        case 1 => histStateEntries.head.docCount.toDouble / queryResult.totalDocuments // normalized on all conversations logs
        case _ => throw ExceptionAtomic("keyword2 atom invalid qaMatchedStatesHistogram")
      }


      var PK_S = 0.0d
      // get keyword freq in queries associated to the current state
      val wordsFreqCurrentState: List[DTStateWordFreqsItem] = DecisionTableService.wordFrequenciesInQueriesByState(data.context.indexName).filter(item => item.state == currentState)
      // check if stats are present for the current state
      if (!wordsFreqCurrentState.isEmpty) PK_S = wordsFreqCurrentState.head.getFreqOfWord(keyword)

      P_K = PK_S * P_S
    }

    Result(score=P_K)
  } // returns elasticsearch score of the highest query in queries
}
