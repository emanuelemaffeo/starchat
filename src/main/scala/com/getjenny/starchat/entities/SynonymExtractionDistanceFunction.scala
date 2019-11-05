package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

import scalaz.Scalaz._

object SynonymExtractionDistanceFunction extends Enumeration {
  type SynExtractionDist = Value
  val EMDCOSINE, SUMCOSINE, MEANCOSINE, unknown = Value
  def value(synExtractionDist: String): SynonymExtractionDistanceFunction.Value =
    values.find(_.toString === synExtractionDist).getOrElse(unknown)
}
