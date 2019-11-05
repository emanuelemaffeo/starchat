package com.getjenny.starchat.services.esclient

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 22/05/18.
 */

object ConversationLogsElasticClient extends QuestionAnswerElasticClient {
  override val indexName: String = ""
  override val indexSuffix: String = convLogsIndexSuffix
  override val dictSizeCacheMaxSize: Int = config.getInt("es.dictSizeCacheMaxSize")
  override val totalTermsCacheMaxSize: Int = config.getInt("es.totalTermsCacheMaxSize")
  override val countTermCacheMaxSize: Int = config.getInt("es.countTermCacheMaxSize")
  override val cacheStealTimeMillis: Int = config.getInt("es.cacheStealTimeMillis")
}
