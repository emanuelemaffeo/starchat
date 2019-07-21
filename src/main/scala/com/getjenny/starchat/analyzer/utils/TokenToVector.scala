package com.getjenny.starchat.analyzer.utils

import breeze.linalg.SparseVector
import breeze.linalg.functions.cosineDistance
import scalaz.Scalaz._

import scala.collection.immutable.{List, Map}

object TokenToVector {
  def tokensToVector(tokens: List[String], tokenIndex: Map[String, Int]): SparseVector[Double]= {
    val (index, data) = tokens.groupBy(identity).mapValues(_.map(_ => 1).sum)
      .map{ case (k, v) =>
        (tokenIndex.getOrElse(k, 0), v.toDouble)
      }.toArray.sortBy{case (valueIdx, _) => valueIdx}.unzip
    new SparseVector(index = index, data = data, length = tokenIndex.length)
  }

  /** Calculate the cosine distance between sparse vectors
    *
    * @param u first vector
    * @param v second vector
    * @return the distance in the range 0.0 - 1.0
    */
  def cosineDist(u: SparseVector[Double], v: SparseVector[Double]): Double = {
    if (u.length =/= v.length) throw new IllegalArgumentException("Vectors have different length")

    /** the range of the function cosineDistance (breeze package) goes from 2.0 (max distance) to 0.0 (min distance)
      * we divide by 2.0 to normalize between 1.0 and 0.0
      */
    cosineDistance(u, v) / 2.0
  }
}
