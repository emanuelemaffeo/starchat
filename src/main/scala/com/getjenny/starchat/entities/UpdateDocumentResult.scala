package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 02/07/16.
  */

case class UpdateDocumentResult(index: String,
                               dtype: String,
                               id: String,
                               version: Long,
                               created: Boolean
                              )

case class UpdateDocumentsResult(data: List[UpdateDocumentResult])
