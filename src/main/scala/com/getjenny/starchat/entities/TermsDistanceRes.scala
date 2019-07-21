package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 08/06/18.
  */

case class TermsDistanceRes(
                             term1: String,
                             term2: String,
                             vector1: Vector[Double],
                             vector2: Vector[Double],
                             cosDistance: Double,
                             eucDistance: Double
                           )
