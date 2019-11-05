package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 15/03/18.
 */

case class QAAggregatedAnalyticsRequest(
                                         interval: Option[String] = None,
                                         minDocInBuckets: Option[Long] = None,
                                         timestampGte: Option[Long] = None,
                                         timestampLte: Option[Long] = None,
                                         aggregations: Option[List[QAAggregationsTypes.Value]] = None,
                                         timezone: Option[String] = None
                                       )
