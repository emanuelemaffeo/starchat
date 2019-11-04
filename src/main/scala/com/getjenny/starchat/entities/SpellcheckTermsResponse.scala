package com.getjenny.starchat.entities

/**
 * Created by angelo on 21/04/17.
 */

case class SpellcheckTokenSuggestions(
                                       score: Double,
                                       freq: Double,
                                       text: String
                                     )

case class SpellcheckToken(
                            text: String,
                            offset: Int,
                            length: Int,
                            options: List[SpellcheckTokenSuggestions]
                          )

case class SpellcheckTermsResponse(
                                    tokens: List[SpellcheckToken]
                                  )
