package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
  */

import scala.collection.immutable.Map

case class ResponseRequestOut(conversation_id: String,
                              state: String,
                              traversed_states: Vector[String],
                              max_state_count: Int,
                              analyzer: String,
                              bubble: String,
                              action: String,
                              data: Map[String, String],
                              action_input: Map[String, String],
                              state_data: Map[String, String],
                              success_value: String,
                              failure_value: String,
                              score: Double,
                              actionResult: Option[DtActionResult] = None
                             )
