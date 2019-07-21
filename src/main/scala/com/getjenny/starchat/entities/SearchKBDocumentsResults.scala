package com.getjenny.starchat.entities

import scala.collection.immutable.{List}

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

case class SearchKBDocument(score: Float, document: KBDocument)

case class SearchKBDocumentsResults(total: Long, max_score: Float, hits: List[SearchKBDocument])

