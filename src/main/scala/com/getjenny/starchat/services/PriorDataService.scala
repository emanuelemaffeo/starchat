package com.getjenny.starchat.services

import com.getjenny.starchat.services.esclient.{PriorDataElasticClient, QuestionAnswerElasticClient}

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

object PriorDataService extends QuestionAnswerService {
  override val elasticClient: QuestionAnswerElasticClient = PriorDataElasticClient
  override var dictSizeCacheMaxSize: Int = elasticClient.dictSizeCacheMaxSize
  override var totalTermsCacheMaxSize: Int = elasticClient.totalTermsCacheMaxSize
  override var countTermCacheMaxSize: Int = elasticClient.countTermCacheMaxSize
  override var cacheStealTimeMillis: Int = elasticClient.cacheStealTimeMillis
}
