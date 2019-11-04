package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

case class DTDocumentSearch(from: Option[Int],
                            size: Option[Int],
                            executionOrder: Option[Int],
                            minScore: Option[Float],
                            boostExactMatchFactor: Option[Float],
                            state: Option[String],
                            evaluationClass: Option[String],
                            queries: Option[String],
                            searchAlgorithm: Option[SearchAlgorithm.Value]
                           )
