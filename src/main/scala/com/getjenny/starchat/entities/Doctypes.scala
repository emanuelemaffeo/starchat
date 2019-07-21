package com.getjenny.starchat.entities

/**
  * Created by angelo on 19/07/19.
  */

import scalaz.Scalaz._

object Doctypes extends Enumeration {
  val canned,

  /** canned document, indexed but retrieved only under particular circumstances */
  hidden,

  /** hidden document, these are indexed but must not be retrieved,
    * use this type for data used just to improve statistic for data retrieval */
  decisiontable,

  /** does not contains conversation data, used to redirect the
    * conversation to any state of the decision tree */
  metadata,

  /** used for metadata e.g. conversation medatada */
  normal = Doctypes.Value

  /** normal document, can be returned to the user as response */
  def value(v: String) = values.find(_.toString === v).getOrElse(normal)
}