package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 06/12/18.
  */

import scalaz.Scalaz._

object SearchAlgorithm extends Enumeration {
  val SHINGLES2,
  SHINGLES3,
  SHINGLES4,
  STEM_SHINGLES2,
  STEM_SHINGLES3,
  STEM_SHINGLES4,
  STEM_BOOST_EXACT,
  NGRAM2,
  STEM_NGRAM2,
  NGRAM3,
  STEM_NGRAM3,
  NGRAM4,
  STEM_NGRAM4,
  AUTO,
  DEFAULT = SearchAlgorithm.Value
  def value(algorithm: String): SearchAlgorithm.Value = values.find(_.toString === algorithm).getOrElse(DEFAULT)
}
