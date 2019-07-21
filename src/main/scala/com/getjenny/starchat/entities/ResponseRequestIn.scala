package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
  */

import scala.collection.immutable.Map

case class ResponseRequestInUserInput(text: Option[String], img: Option[String])

case class ResponseRequestInValues(return_value: Option[String], data: Option[Map[String, String]])

case class ResponseRequestIn(conversation_id: String,
                             traversed_states: Option[Vector[String]],
                             user_input: Option[ResponseRequestInUserInput],
                             state: Option[List[String]] = None,
                             values: Option[ResponseRequestInValues],
                             threshold: Option[Double],
                             max_results: Option[Int],
                             search_algorithm: Option[SearchAlgorithm.Value] = None)
