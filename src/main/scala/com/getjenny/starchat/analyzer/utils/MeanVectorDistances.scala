package com.getjenny.starchat.analyzer.utils

import com.getjenny.analyzer.util.VectorUtils
import com.getjenny.starchat.entities._
import com.getjenny.starchat.services._

/**
  * Created by angelo on 06/09/18.
  */

object MeanVectorDistances {
  val termService: TermService.type = TermService

  def distanceCosine(textTerms1: TextTerms, textTerms2: TextTerms): Double = {
    val text1V = TextToVectorsTools.meanOfTermsVectors(textTerms1)
    val text2V = TextToVectorsTools.meanOfTermsVectors(textTerms2)
    (text1V, text2V) match {
      case ((vector1, mean1), (vector2, mean2)) =>
        val distance = VectorUtils.cosineDist(vector1, vector2)
        (1 - distance) * (mean1 * mean2)
    }
  }
}
