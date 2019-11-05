package com.getjenny.starchat.entities

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
