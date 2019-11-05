package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

import scalaz.Scalaz._

object TermCountFields extends Enumeration {
  type TermCountField = Value
  val question, answer, all, unknown = Value
  def value(termCountField: String): TermCountFields.Value =
    values.find(_.toString === termCountField).getOrElse(unknown)
}

case class TermCount(numDocs: Long, count: Long)
