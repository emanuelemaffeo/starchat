package com.getjenny.starchat.services.esclient

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

object DecisionTableElasticClient extends ElasticClient {
  override val indexName: String = ""
  override val indexSuffix: String = dtIndexSuffix
  val queryMinThreshold : Float = config.getDouble("es.dt_query_min_threshold").toFloat
  val boostExactMatchFactor : Float = config.getDouble("es.dt_boost_exact_match_factor").toFloat
  val queriesScoreMode: String = config.getString("es.dt_queries_score_mode").toLowerCase
}
