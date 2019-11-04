package com.getjenny.starchat.entities

import scala.collection.immutable.List

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

case class SearchQADocument(score: Float, document: QADocument)

case class SearchQADocumentsResults(totalHits: Long = 0,
                                    hitsCount: Long = 0,
                                    maxScore: Float = 0.0f,
                                    hits: List[SearchQADocument] = List.empty[SearchQADocument])
