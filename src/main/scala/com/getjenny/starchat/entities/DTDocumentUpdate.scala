package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
 */

import scala.collection.immutable.{List, Map}

case class DTDocumentUpdate(state: String,
                            executionOrder: Option[Int],
                            maxStateCount: Option[Int],
                            analyzer: Option[String],
                            queries: Option[List[String]],
                            bubble: Option[String],
                            action: Option[String],
                            actionInput: Option[Map[String, String]],
                            stateData: Option[Map[String, String]],
                            successValue: Option[String],
                            failureValue: Option[String],
                            evaluationClass: Option[String]
                           )
