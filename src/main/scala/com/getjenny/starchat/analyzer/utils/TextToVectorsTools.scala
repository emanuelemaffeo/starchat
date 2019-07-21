package com.getjenny.starchat.analyzer.utils

/**
  * Created by angelo on 11/04/17.
  */

import com.getjenny.analyzer.util.VectorUtils._
import com.getjenny.starchat.entities._
import com.getjenny.starchat.services._

object TextToVectorsTools {
  val termService: TermService.type = TermService
  def emptyVec(length: Int = 300): Vector[Double] = Vector.fill(length){0.0}

  implicit class Crosstable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  def textTermsToVectors(textTerms: TextTerms): List[(String, Vector[Double])] = {
    val termsList = textTerms.terms.getOrElse(Terms(terms = List.empty[Term]))
    termsList.terms.map(e => (e.term, e.vector.getOrElse(TextToVectorsTools.emptyVec())))
  }

  def meanOfTermsVectors(textTerms: TextTerms, length: Int = 300): (Vector[Double], Double) = {
    val termsList = textTerms.terms.getOrElse(Terms(terms = List.empty[Term]))
    val vectors = termsList.terms.map(e => e.vector.getOrElse(emptyVec())).toVector
    val sentenceVector = if (vectors.nonEmpty)
      meanArrayOfArrays(vectors)
    else
      emptyVec(length)

    val reliabilityFactor =
      textTerms.terms_found_n.toDouble / math.max(1, textTerms.text_terms_n.toDouble)
    (sentenceVector, reliabilityFactor)
  }

  def sumOfTermsVectors(textTerms: TextTerms, length: Int = 300): (Vector[Double], Double) = {
    val termsList = textTerms.terms.getOrElse(Terms(terms = List.empty[Term]))
    val vectors = termsList.terms.map(e => e.vector.getOrElse(emptyVec())).toVector
    val sentenceVector = if (vectors.nonEmpty)
      sumArrayOfArrays(vectors)
    else
      emptyVec(length)

    val reliabilityFactor =
      textTerms.terms_found_n.toDouble / math.max(1, textTerms.text_terms_n.toDouble)
    (sentenceVector, reliabilityFactor)
  }

  def sumOfVectorsFromText(index_name: String, text: String): (Vector[Double], Double) = {
    val textVectors = termService.textToVectors(index_name, text)
    val vector = sumOfTermsVectors(textVectors)
    vector
  }

}
