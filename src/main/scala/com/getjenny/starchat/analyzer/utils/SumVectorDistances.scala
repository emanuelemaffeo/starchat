package com.getjenny.starchat.analyzer.utils

import com.getjenny.analyzer.util.VectorUtils
import com.getjenny.starchat.entities._
import com.getjenny.starchat.services._

/**
  * Created by angelo on 04/04/17.
  */

object SumVectorDistances {
  val termService: TermService.type = TermService

  def distanceCosine(textTerms1: TextTerms, textTerms2: TextTerms): Double = {
    val text1V = TextToVectorsTools.sumOfTermsVectors(textTerms1)
    val text2V = TextToVectorsTools.sumOfTermsVectors(textTerms2)
    (text1V, text2V) match {
      case ((vector1, sum1), (vector2, sum2)) =>
        val distance = VectorUtils.cosineDist(vector1, vector2)
        (1 - distance) * (sum1 * sum2)
    }
  }
}
