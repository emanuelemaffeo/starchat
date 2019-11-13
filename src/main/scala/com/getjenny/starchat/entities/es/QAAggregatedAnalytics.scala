package com.getjenny.starchat.entities.es

import java.time.ZonedDateTime

import com.getjenny.starchat.entities.QAAggregationsTypes
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter
import org.elasticsearch.search.aggregations.bucket.histogram.{Histogram, ParsedDateHistogram}
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.elasticsearch.search.aggregations.metrics.{Avg, Cardinality}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 15/03/19.
 */

case class ScoreHistogramItem(
                               key: String,
                               docCount: Long
                             )

case class LabelCountHistogramItem(
                                    key: String,
                                    docCount: Long
                                  )

case class CountOverTimeHistogramItem(
                                       key: Long,
                                       keyAsString: String,
                                       docCount: Long
                                     )

case class AvgScoresHistogramItem(
                                   keyAsString: String,
                                   key: Long,
                                   docCount: Long,
                                   avgScore: Double
                                 )

case class QAAggregatedAnalytics(
                                  totalDocuments: Long = 0,
                                  totalConversations: Long = 0,
                                  avgFeedbackConvScore: Option[Double] = None,
                                  avgAlgorithmConvScore: Option[Double] = None,
                                  avgAlgorithmAnswerScore: Option[Double] = None,
                                  avgFeedbackAnswerScore: Option[Double] = None,
                                  scoreHistograms: Option[Map[String, List[ScoreHistogramItem]]] = None,
                                  labelCountHistograms: Option[Map[String, List[LabelCountHistogramItem]]] = None,
                                  countOverTimeHistograms: Option[Map[String, List[CountOverTimeHistogramItem]]] = None,
                                  scoresOverTime: Option[Map[String, List[AvgScoresHistogramItem]]] = None
                                )

class QAAggregatedAnalyticsEntityManager(aggregationsTypes: Option[List[QAAggregationsTypes.Value]]) extends ReadEntityManager[QAAggregatedAnalytics] {
  override def fromSearchResponse(response: SearchResponse): List[QAAggregatedAnalytics] = {
    val totalDocuments: Cardinality = response.getAggregations.get("totalDocuments")
    val totalConversations: Cardinality = response.getAggregations.get("totalConversations")

    aggregationsTypes match {
      case Some(aggregationsReq) =>
        val reqAggs = aggregationsReq.toSet
        val avgFeedbackConvScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackConvScore)) {
          val avg: Avg = response.getAggregations.get("avgFeedbackConvScore")
          Some(avg.getValue)
        } else None
        val avgFeedbackAnswerScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgFeedbackAnswerScore)) {
          val avg: Avg = response.getAggregations.get("avgFeedbackAnswerScore")
          Some(avg.getValue)
        } else None
        val avgAlgorithmConvScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmConvScore)) {
          val avg: Avg = response.getAggregations.get("avgAlgorithmConvScore")
          Some(avg.getValue)
        } else None
        val avgAlgorithmAnswerScore: Option[Double] = if (reqAggs.contains(QAAggregationsTypes.avgAlgorithmAnswerScore)) {
          val avg: Avg = response.getAggregations.get("avgAlgorithmAnswerScore")
          Some(avg.getValue)
        } else None
        val scoreHistogram: Option[List[ScoreHistogramItem]] = if (reqAggs.contains(QAAggregationsTypes.scoreHistogram)) {
          val h: Histogram = response.getAggregations.get("scoreHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("scoreHistogramNotTransferred")
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
          val pf: ParsedFilter = response.getAggregations.get("scoreHistogramTransferred")
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
          val pf: ParsedFilter = response.getAggregations.get("conversationsHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("conversationsNotTransferredHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("conversationsTransferredHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("qaPairHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("qaPairAnsweredHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("qaPairUnansweredHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("qaMatchedStatesHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("qaMatchedStatesWithScoreHistogram")
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
          val pf: ParsedFilter = response.getAggregations.get("avgFeedbackNotTransferredConvScoreOverTime")
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
          val pf: ParsedFilter = response.getAggregations.get("avgFeedbackTransferredConvScoreOverTime")
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
          val pf: ParsedFilter = response.getAggregations.get("avgAlgorithmNotTransferredConvScoreOverTime")
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
          val pf: ParsedFilter = response.getAggregations.get("avgAlgorithmTransferredConvScoreOverTime")
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
          val h: ParsedDateHistogram = response.getAggregations.get("avgFeedbackConvScoreOverTime")
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
          val h: ParsedDateHistogram = response.getAggregations.get("avgAlgorithmAnswerScoreOverTime")
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
          val h: ParsedDateHistogram = response.getAggregations.get("avgFeedbackAnswerScoreOverTime")
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
          val h: ParsedDateHistogram = response.getAggregations.get("avgAlgorithmConvScoreOverTime")
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
          "qaMatchedStatesWithScoreHistogram" -> qaMatchedStatesWithScoreHistogram
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

        List(QAAggregatedAnalytics(totalDocuments = totalDocuments.getValue,
          totalConversations = totalConversations.getValue,
          avgFeedbackConvScore = avgFeedbackConvScore,
          avgFeedbackAnswerScore = avgFeedbackAnswerScore,
          avgAlgorithmConvScore = avgAlgorithmConvScore,
          avgAlgorithmAnswerScore = avgAlgorithmAnswerScore,
          labelCountHistograms = if (labelCountHistograms.nonEmpty) Some(labelCountHistograms) else None,
          scoreHistograms = if (scoreHistograms.nonEmpty) Some(scoreHistograms) else None,
          countOverTimeHistograms = if (countOverTimeHistograms.nonEmpty) Some(countOverTimeHistograms) else None,
          scoresOverTime = if (scoresOverTime.nonEmpty) Some(scoresOverTime) else None
        ))
      case _ =>
        List(QAAggregatedAnalytics(totalDocuments = totalDocuments.getValue,
          totalConversations = totalConversations.getValue))
    }

  }

  override def fromGetResponse(response: List[GetResponse]): List[QAAggregatedAnalytics] = {
    throw new NotImplementedException()
  }
}
