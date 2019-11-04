package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 10/03/2017.
 */
case class LanguageGuesserRequestOut(language: String,
                                     score: Float,
                                     confidence: String,
                                     enoughText: Boolean
                                    )
