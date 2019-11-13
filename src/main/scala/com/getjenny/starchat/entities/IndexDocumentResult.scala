package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 02/07/16.
 */

case class IndexDocumentResult(index: String,
                               id: String,
                               version: Long,
                               created: Boolean
                              )

case class IndexDocumentListResult(data: List[IndexDocumentResult])
