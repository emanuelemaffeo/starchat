package com.getjenny.starchat.services.esclient

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 22/05/18.
 */

trait QuestionAnswerElasticClient extends ElasticClient {
  val queryMinThreshold : Float = config.getDouble("es.kb_query_min_threshold").toFloat
  val queriesScoreMode: String = config.getString("es.kb_nested_score_mode").toLowerCase
  val questionNegativeMinimumMatch: String = config.getString("es.kb_question_negative_minimum_match")
  val questionNegativeBoost: Float = config.getDouble("es.kb_question_negative_boost").toFloat
  val questionExactMatchBoost: Float = config.getDouble("es.kb_question_exact_match_boost").toFloat
  val dictSizeCacheMaxSize: Int
  val totalTermsCacheMaxSize: Int
  val countTermCacheMaxSize: Int
  val cacheStealTimeMillis: Int
}
