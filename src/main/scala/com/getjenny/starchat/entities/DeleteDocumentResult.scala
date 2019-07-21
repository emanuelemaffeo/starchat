package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 02/07/16.
  */

case class DeleteDocumentResult(index: String,
                                dtype: String,
                                id: String,
                                version: Long,
                                found: Boolean
                               )

case class DeleteDocumentsResult(data: List[DeleteDocumentResult])

case class DeleteDocumentsSummaryResult(message: String, deleted: Long)
