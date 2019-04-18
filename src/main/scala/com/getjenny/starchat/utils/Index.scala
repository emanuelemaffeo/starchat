package com.getjenny.starchat.utils

import com.getjenny.starchat.services.TermService

import scala.util.matching.Regex

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 19/07/18.
  */

object Index {

  private[this] val langRegex: String = "[a-z]{1,256}"
  private[this] val arbitraryPatternRegex: String = "[a-z0-9_]{1,256}"

  /** regular expression to match index names */
  val systemIndexMatchRegex: Regex = "(starchat_system_[a-z0-9\\-]{1,256})".r
  val systemIndexMatchRegexDelimited: Regex = ("^" + systemIndexMatchRegex + "$").r

  val indexMatchRegex: Regex = ("(index_(?:" + langRegex + ")_(?:" + arbitraryPatternRegex + "))").r
  val indexMatchRegexDelimited: Regex = ("^" + indexMatchRegex + "$").r

  val indexExtractFieldsRegex: Regex = ("""(?:index_(""" + langRegex + ")_(" + arbitraryPatternRegex + "))").r
  val indexExtractFieldsRegexDelimited: Regex = ("^" + indexExtractFieldsRegex + "$").r

  /** Extract language from index name
    *
    * @param indexName the full index name
    * @return a tuple with the two component of the index (org, language, arbitrary)
    */
  def patternsFromIndexName(indexName: String): (String, String) = {
    val (language, arbitrary) = indexName match {
      case indexExtractFieldsRegexDelimited(languagePattern, arbitraryPattern) =>
        (languagePattern, arbitraryPattern)
      case _ => throw new Exception("index name is not well formed")
    }
    (language, arbitrary)
  }

  /** calculate and return the full index name
    *
    * @param indexName the index name
    * @param suffix the suffix name
    * @return the full index name made of indexName and Suffix
    */
  def indexName(indexName: String, suffix: String): String = {
    if(suffix.nonEmpty) {
      indexName + "." + suffix
    } else indexName
  }
}
