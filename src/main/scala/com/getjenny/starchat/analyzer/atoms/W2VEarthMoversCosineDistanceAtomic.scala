package com.getjenny.starchat.analyzer.atoms

import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import com.getjenny.starchat.analyzer.utils.EMDVectorDistances
import com.getjenny.starchat.services._

/**
  * Created by angelo on 04/04/17.
  */

class W2VEarthMoversCosineDistanceAtomic(val arguments: List[String], restrictedArgs: Map[String, String])
  extends AbstractAtomic  {
  /**
    * cosine distance between sentences renormalized at [0, 1]: (cosine + 1)/2
    *
    * state_lost_password_cosine = Cosine("lost password")
    * state_lost_password_cosine.evaluate("I'm desperate, I've lost my password")
    *
    */

  val sentence: String = arguments.headOption match {
    case Some(t) => t
    case _ =>
      throw ExceptionAtomic("similarCosEmd requires an argument")
  }

  val termService: TermService.type = TermService

  implicit class Crosstable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
  }

  override def toString: String = "similarCosEmd(\"" + sentence + "\")"
  val isEvaluateNormalized: Boolean = true

  val indexName: String = restrictedArgs("index_name")

  def evaluate(query: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {
    val emdDist = EMDVectorDistances.distanceCosine(indexName, query, sentence)
    Result(score=emdDist)
  }

  // Similarity is normally the cosine itself. The threshold should be at least

  // angle < pi/2 (cosine > 0), but for synonyms let's put cosine > 0.6, i.e. self.evaluate > 0.8
  override val matchThreshold: Double = 0.8
}
