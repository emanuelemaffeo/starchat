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
  val isEvaluateNormalized: Boolean = false
  override val matchThreshold: Double = 0.6

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
      Result(score=0.0d)
    }
    else {
      var PS_K = 0.0d
      var PK_S = 0.0d
      var P_K = 0.0d
      var P_S = 0.0d
      val currentStateName = data.context.stateName
      val indexName = data.context.indexName;
      val currentState: DecisionTableRuntimeItem = AnalyzerService.analyzersMap(indexName).analyzerMap.get(currentStateName) match
      {
        case Some(t) => t
        case _  => throw ExceptionAtomic("Keyword atom: state not found in map")
      }

      // Evaluation of PK_S
      if (currentState.queries.isEmpty) {
        PK_S = 0.5d
      }
      else {
        PK_S = fractionOfQueriesWithWordOfState(indexName,keyword,currentStateName)
      }

      // Evaluation of P_S
      P_S = stateFrequency(indexName,currentStateName)

      //Evaluation of P_K
      P_K = KeywordAbsoluteProbability(indexName,keyword)



      PS_K = PK_S * P_S / P_K

      var msg = "index(" + indexName + ") : state(" + currentStateName + ")"
      msg += " : PK_S(" + PK_S + ")"
      msg += " : PK(" + P_K + ")"
      msg += " : PS(" + P_S + ")"
      msg += " : PS_K(" + PS_K + ")"
      log.info(msg)


      Result(score=PS_K)
    }

  }
}
