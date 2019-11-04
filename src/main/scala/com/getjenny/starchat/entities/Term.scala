package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 11/07/17.
 */

import scala.collection.immutable.Map

case class Term(term: String,
                synonyms: Option[Map[String, Double]] = None,
                antonyms: Option[Map[String, Double]] = None,
                tags: Option[String] = None,
                features: Option[Map[String, String]] = None,
                frequencyBase: Option[Double] = None,
                frequencyStem: Option[Double] = None,
                vector: Option[Vector[Double]] = None,
                score: Option[Double] = None
               )

case class SearchTerm(term: Option[String] = None,
                      synonyms: Option[Map[String, Double]] = None,
                      antonyms: Option[Map[String, Double]] = None,
                      tags: Option[String] = None,
                      features: Option[Map[String, String]] = None,
                      frequencyBase: Option[Double] = None,
                      frequencyStem: Option[Double] = None,
                      vector: Option[Vector[Double]] = None,
                      score: Option[Double] = None
                     )

case class Terms(terms: List[Term])

case class TermsResults(total: Int, maxScore: Float, hits: Terms)

case class TextTerms(
                      text: String,
                      textTermsN: Int,
                      termsFoundN: Int,
                      terms: Terms
                    )
