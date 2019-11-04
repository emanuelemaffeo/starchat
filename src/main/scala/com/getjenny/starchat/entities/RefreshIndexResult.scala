package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 24/03/17.
 */

case class FailedShard(indexName: String,
                       shardId: Int,
                       reason: String,
                       status: Int
                      )

case class RefreshIndexResult(indexName: String,
                              failedShardsN: Int,
                              successfulShardsN: Int,
                              totalShardsN: Int,
                              failedShards: List[FailedShard]
                             )

case class RefreshIndexResults(results: List[RefreshIndexResult])
