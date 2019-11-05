package com.getjenny.starchat.entities

/**
 * Created by angelo@getjenny.com on 21/04/17.
 */

case class SpellcheckTermsRequest(
                                   text: String,
                                   prefixLength: Int = 3,
                                   minDocFreq: Int = 1,
                                   maxEdit: Int = 2
                                 )

