package com.getjenny.starchat.services.esclient

import com.getjenny.starchat.services.esclient.SystemIndexManagementElasticClient.config

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

object BayesOperatorCacheElasticClient extends ElasticClient {
  override val indexName: String = config.getString("es.system_index_name")
  override val indexSuffix: String = systemBayesOperatorCacheIndexSuffix
}
