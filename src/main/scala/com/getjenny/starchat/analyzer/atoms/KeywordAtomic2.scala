package com.getjenny.starchat.analyzer.atoms

/**
  * Created by mal on 20/02/2017.
  */

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import com.getjenny.starchat.SCActorSystem
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
  val isEvaluateNormalized: Boolean = true
  override val matchThreshold: Double = 0.0

  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)
  val decisionTableService: DecisionTableService.type = DecisionTableService
  val conversationLogsService: ConversationLogsService.type  = ConversationLogsService

  private def fractionOfQueriesWithWordOfState(indexName: String, word: String, state: String): Double =
  {
    // get keyword freq in queries associated to the current state
    val wordsFreqCurrentState: List[DTStateWordFreqsItem] = DecisionTableService.wordFrequenciesInQueriesByState(indexName).filter(item => item.state == state)
    // check if stats are present for the current state
    if (!wordsFreqCurrentState.isEmpty) wordsFreqCurrentState.head.getFreqOfWord(word)
    else 0.0d
  }
  private def wordFrequencyInQueriesFieldOfAllStates(indexName: String, word: String): Double =
  {
    // get keyword freq in all queries
    val wordsFreq: List[DTWordFreqItem] = DecisionTableService.wordFrequenciesInQueries(indexName).filter(item => item.word == word)
    // check if word is present in the histogram
    if (!wordsFreq.isEmpty) wordsFreq.head.freq
    else 0.0d
  }

  private def KeywordAbsoluteProbability(indexName: String, word: String): Double = {
    val freq = wordFrequencyInQueriesFieldOfAllStates(indexName, keyword)
    if (freq == 0.0d)
      0.5d
    else
      freq
  }

  private def stateFrequency(indexName:String, stateName: String): Double =
  {
    val nStates =AnalyzerService.analyzersMap(indexName).analyzerMap.size
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
    val histStateEntries = stateFrequenciesHist.filter(elem => elem.key == stateName)
    val P_S: Double = histStateEntries.length match {
      case 0 => 1.0d/nStates
      case 1 => histStateEntries.head.docCount.toDouble / queryResult.totalDocuments // normalized on all conversations logs
      case _ => throw ExceptionAtomic("keyword2 atom invalid qaMatchedStatesHistogram")
    }
    P_S
  }

  def evaluate(userQuery: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {

    if(!userQuery.contains(keyword)) {
      //keyword not present in the user's query: return 0, whatever the other conditions are
      Result(score=0.0d)
    }
    else {
      var result = 0.0d

      val currentStateName = data.context.stateName
      val indexName = data.context.indexName;
      val currentState: DecisionTableRuntimeItem = AnalyzerService.analyzersMap(indexName).analyzerMap.get(currentStateName) match
      {
        case Some(t) => t
        case _  => throw ExceptionAtomic("Keyword atom: state not found in map")
      }

      val P_S = stateFrequency(indexName,currentStateName)

      //  keyword present in the user's query but not in the state's queries: return P(S) (because we cannot return 0)
      if (currentState.queries.isEmpty) {
        result = P_S
      }
      else {
        val PK_S = fractionOfQueriesWithWordOfState(indexName,keyword,currentStateName)
        val P_K = KeywordAbsoluteProbability(indexName,keyword)
        // keyword present in the user's query but not in the state's queries: return P(S) (because we cannot return 0)
        if (PK_S == 0 || P_K == 0)
          result = P_S
        else {
          result = PK_S * P_S / P_K
        }

        var msg = "index(" + indexName + ") : state(" + currentStateName + ")"
        msg += " : PK_S(" + PK_S + ")"
        msg += " : PK(" + P_K + ")"
        msg += " : PS(" + P_S + ")"
        msg += " : PS_K(" + result + ")"
        log.info(msg)
      }


      Result(score=result)
    }

  }
}
