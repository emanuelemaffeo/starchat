package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 30/04/18.
 */

import scalaz.Scalaz._

object ObservedDataSources extends Enumeration {
  type ObservedDataSource = Value
  val KNOWLEDGEBASE, CONV_LOGS, unknown = Value
  def value(enumValue: String) = values.find(_.toString === enumValue).getOrElse(unknown)
}

