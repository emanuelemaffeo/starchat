package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

case class SynonymItem (
                         synonym: String, /** the synonym text */
                         synonymScore: Double, /** the similarity score between this token and the original,
                                it can be different from the other score this field should be
                                used to select the most similar term */
                         termSimilarityScore: Double, /** cosine distance between the synonym and the original term */
                         textDistanceWithSynonym: Double /** the cosine distance between the vectorial
                                      sentence representation with and without the synonym */
                       )
