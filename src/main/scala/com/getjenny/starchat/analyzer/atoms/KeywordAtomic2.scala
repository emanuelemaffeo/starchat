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
import scalaz.Scalaz._
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

  private[this] def fractionOfQueriesWithWordOfState(indexName: String, word: String, state: String): Double =
  {
    // get keyword freq in queries associated to the current state
    val wordsFreqCurrentState: List[DTStateWordFreqsItem] = DecisionTableService.wordFrequenciesInQueriesByState(indexName).filter(item => item.state === state)
    // check if stats are present for the current state

    val res: Double =  wordsFreqCurrentState.headOption match {
      case Some(t) => t.getFreqOfWord(word)
      case _  => 0.0d
    }
    res
  }

  private[this] def wordFrequencyInQueriesFieldOfAllStates(indexName: String, word: String): Double =
  {
    // get keyword freq in all queries
    val wordsFreq: List[DTWordFreqItem] = DecisionTableService.wordFrequenciesInQueries(indexName).filter(item => item.word === word)
    // check if word is present in the histogram

    val res: Double =  wordsFreq.headOption match {
      case Some(t) => t.freq
      case _  => 0.0d
    }
    res
  }

  private[this] def keywordAbsoluteProbability(indexName: String, word: String): Double = {
    val freq = wordFrequencyInQueriesFieldOfAllStates(indexName, keyword)
    if (freq === 0.0d)
      0.5d
    else
      freq
  }

  private[this] def stateFrequency(indexName:String, stateName: String): Double =
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
    val histStateEntries = stateFrequenciesHist.filter(elem => elem.key === stateName)

    val stateHits: Double = histStateEntries.headOption  match
    {
      case Some(t) => t.docCount
      case _ => 0
    }

    val probS = (stateHits+100.0d) / (100.0d*nStates + queryResult.totalDocuments)
    probS
  }

  def evaluate(userQuery: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {

    if(!userQuery.contains(keyword)) {
      //keyword not present in the user's query: return 0, whatever the other conditions are
      Result(score=0.0d)
    }
    else {

      val currentStateName = data.context.stateName
      val indexName = data.context.indexName;
      val currentState: DecisionTableRuntimeItem = AnalyzerService.analyzersMap(indexName).analyzerMap.get(currentStateName) match
      {
        case Some(t) => t
        case _  => throw ExceptionAtomic("Keyword atom: state not found in map")
      }

      val pS= stateFrequency(indexName,currentStateName)

      //  keyword present in the user's query but not in the state's queries: return P(S) (because we cannot return 0)
      if (currentState.queries.isEmpty) {
        Result(score=pS)
      }
      else {
        val pKS = fractionOfQueriesWithWordOfState(indexName,keyword,currentStateName)
        val pK = keywordAbsoluteProbability(indexName,keyword)
        // keyword present in the user's query but not in the state's queries: return P(S) (because we cannot return 0)
        if (pKS === 0 || pK === 0) {
          val msg = "K2.0 Result(" + pS + ") : state(" + currentStateName + ") : PK_S(" + pKS + ") : PK(" + pK + ") : PS(" + pS + ")"
          log.info(msg)
          Result(score = pS)
        }
        else {
          val pSK = pKS * pS / pK

          val msg = "K2.0 Result(" + pSK + ") : state(" + currentStateName + ") : PK_S(" + pKS + ") : PK(" + pK + ") : PS(" + pS + ") : PS_K(" + pSK + ")"
          log.info(msg)

          Result(score=pSK)
        }

      }

      Result(score=0.0d)
    }

  }
}
