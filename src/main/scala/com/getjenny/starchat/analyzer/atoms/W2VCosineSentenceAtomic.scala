package com.getjenny.starchat.analyzer.atoms

import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import com.getjenny.analyzer.util.VectorUtils._
import com.getjenny.starchat.analyzer.utils.TextToVectorsTools
import com.getjenny.starchat.services._

/**
  * Created by mal on 20/02/2017.
  */

class W2VCosineSentenceAtomic(val arguments: List[String],
                              restrictedArgs: Map[String, String]) extends AbstractAtomic  {
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
      throw ExceptionAtomic("cosineSentence requires an argument")
  }

  val termService: TermService.type = TermService

  override def toString: String = "similar(\"" + sentence + "\")"
  val isEvaluateNormalized: Boolean = true

  val indexName: String = restrictedArgs("index_name")

  val sentenceVector: (Vector[Double], Double) = TextToVectorsTools.sumOfVectorsFromText(indexName, sentence)

  def evaluate(query: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {
    val query_vector = TextToVectorsTools.sumOfVectorsFromText(indexName, query)

    /** cosineDist returns 0.0 for the closest vector, we want 1.0 when the similarity is the highest
      *   so we use 1.0 - ...
      */
    val distance = (1.0 - cosineDist(sentenceVector._1, query_vector._1)) *
      (sentenceVector._2 * query_vector._2) /** <-- these terms are 1.0 when all vector for all terms of the sentence were found */
    Result(score=distance)
  }

  // Similarity is normally the cosine itself. The threshold should be at least
  // angle < pi/2 (cosine > 0), but for synonyms let's put cosine > 0.6, i.e. self.evaluate > 0.8
  override val matchThreshold: Double = 0.8
}
