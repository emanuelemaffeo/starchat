package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 2/03/18.
 */

case class OpenCloseIndex(
                           indexName: String,
                           indexSuffix: String,
                           fullIndexName: String,
                           operation: String,
                           acknowledgement: Boolean
                         )