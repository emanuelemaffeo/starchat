package com.getjenny.starchat.entities

import scala.collection.immutable.List

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

case class SearchDTDocument(score: Float, document: DTDocument)

case class SearchDTDocumentsResults(total: Int, maxScore: Float, hits: List[SearchDTDocument])

