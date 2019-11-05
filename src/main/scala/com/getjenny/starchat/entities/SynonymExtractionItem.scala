package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

case class SynonymExtractionItem(
                                  token: TokenizerResponseItem, /** a token element with position and offset annotation */
                                  isKeywordToken : Boolean = false, /** if the term was selected as relevant
                                          by Manaus or keyword extraction algorithm */
                                  keywordExtractionScore: Double = 0.0d, /** Manaus or keyword extraction algorithm score,
                                                                      only relevant if manausToken is true */
                                  synonymItem: List[SynonymItem] /** list of suggested synonyms */
                                )