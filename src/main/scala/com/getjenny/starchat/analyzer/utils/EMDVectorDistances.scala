package com.getjenny.starchat.analyzer.utils

import com.getjenny.analyzer.util.VectorUtils._
import com.getjenny.starchat.entities._
import com.getjenny.starchat.entities.es.TextTerms
import com.getjenny.starchat.services._

/**
  * Created by angelo on 04/04/17.
  */

object EMDVectorDistances {
  val termService: TermService.type = TermService

  implicit class CrossTable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  //reduced EMD
  private[this] def distanceReducedEMD(textTerms1: TextTerms, textTerms2: TextTerms,
                                       dist_f: (Vector[Double], Vector[Double]) => Double): (Double, Double, Double) = {

    val vectors1 = TextToVectorsTools.textTermsToVectors(textTerms1)
    val vectors2 = TextToVectorsTools.textTermsToVectors(textTerms2)

    val reliabilityFactor1 = textTerms1.termsFoundN.toDouble / math.max(textTerms1.textTermsN.toDouble, 1.0)
    val reliabilityFactor2 = textTerms2.termsFoundN.toDouble / math.max(textTerms2.textTermsN.toDouble, 1.0)

    val words1 = vectors1.groupBy{case(term, _) => term}
      .map{case(term, termVectorPair) =>
        val termFirstEntryVector = termVectorPair.headOption match {
          case Some(vectorPair) => vectorPair match { case (_, vector) => vector }
          case _ => TextToVectorsTools.emptyVec()
        }
        (term, (termVectorPair.length.toDouble, termFirstEntryVector))
      }

    val words2 = vectors2.groupBy{case(term, _) => term}
      .map{case(term, termVectorPair) =>
        val termFirstEntryVector = termVectorPair.headOption match {
          case Some(vectorPair) => vectorPair match { case (_, vector) => vector }
          case _ => TextToVectorsTools.emptyVec()
        }
        (term, (termVectorPair.length.toDouble, termFirstEntryVector))
      }

    if (words1.isEmpty && words2.isEmpty) {
      (1.0, 1.0, 1.0)
    } else if (words1.isEmpty || words2.isEmpty) {
      (0.0, 0.0, 0.0)
    } else {
      val weightedWords1 = words1.map{case(term, (numOfWords, vector)) => (term, (numOfWords / words2.size, vector))}
      val weightedWords2 = words2.map{case(term, (numOfWords, vector)) => (term, (numOfWords / words2.size, vector))}

      val workFromVtoU = weightedWords1.map { case (term1, (weight1, vector1)) =>
        weightedWords2.map({ case (term2, (weight2, vector2)) =>
          val distance: Double = dist_f(vector1, vector2)
          (term1, term2, weight1, weight2, vector1, vector2, distance, weight1 * distance)
        }).minBy{case (_, _, _, _, _, _, distance, _) => distance} match {
          case (_, _, _, _, _, _, _, minWeightedDist) => minWeightedDist
        }
      }.map(x => math.abs(x)).sum

      val workFromUtoV = weightedWords2.map { case (term1, (weight1, vector1)) =>
        weightedWords1.map({ case (term2, (weight2, vector2)) =>
          val distance: Double = dist_f(vector1, vector2)
          (term1, term2, weight1, weight2, vector1, vector2, distance, weight1 * distance)
        }).minBy{case (_, _, _, _, _, _, distance, _) => distance} match {
          case (_, _, _, _, _, _, _, minWeightedDist) => minWeightedDist
        }
      }.map(x => math.abs(x)).sum

      val dist = math.max(workFromUtoV, workFromVtoU)
      /*println("Info: work_from_u_to_v("
        + workFromUtoV + ") work_from_v_to_u(" + workFromVtoU + ") dist(" + dist + ")"
        + " reliability_factor1(" + reliabilityFactor1 + ")"
        + " reliability_factor2(" + reliabilityFactor2 + ")"
      )*/
      (dist, reliabilityFactor1, reliabilityFactor2)
    }
  }

  //reduced EMD
  def distanceText(indexName: String, text1: String, text2: String,
                   dist_f: (Vector[Double], Vector[Double]) => Double): (Double, Double, Double) = {
    val textVectors1 = termService.textToVectors(indexName, text = text1)
    val textVectors2 = termService.textToVectors(indexName, text = text2)
    distanceReducedEMD(textVectors1, textVectors2, dist_f)
  }

  def distanceEuclidean(indexName: String, text1: String, text2: String): Double = {
    val (distanceScore, reliabilityFactor1, reliabilityFactor2) =
      distanceText(indexName =  indexName, text1 = text1, text2 = text2, euclideanDist)
    val score = (1.0 / (1 + distanceScore)) * (reliabilityFactor1 * reliabilityFactor2)
    score
  }

  def distanceCosine(indexName: String, text1: String, text2: String): Double = {
    val (distanceScore, reliabilityFactor1, reliabilityFactor2) =
      distanceText(indexName = indexName, text1 = text1, text2 = text2, cosineDist)
    val score = (1.0 / (1 + distanceScore)) * (reliabilityFactor1 * reliabilityFactor2)
    score
  }

  def distanceEuclidean(textTerms1: TextTerms, textTerms2: TextTerms): Double = {
    val (distanceScore, reliabilityFactor1, reliabilityFactor2) = distanceReducedEMD(textTerms1, textTerms2, euclideanDist)
    val score = (1.0 / (1 + distanceScore)) * (reliabilityFactor1 * reliabilityFactor2)
    score
  }

  def distanceCosine(textTerms1: TextTerms, textTerms2: TextTerms): Double = {
    val (distanceScore, reliabilityFactor1, reliabilityFactor2) = distanceReducedEMD(textTerms1, textTerms2, cosineDist)
    val score = (1.0 / (1 + distanceScore )) * (reliabilityFactor1 * reliabilityFactor2)
    score
  }
}
