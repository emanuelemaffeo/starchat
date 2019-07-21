package com.getjenny.starchat.analyzer.atoms

import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import com.getjenny.analyzer.util.VectorUtils._
import com.getjenny.starchat.analyzer.utils.TextToVectorsTools
import com.getjenny.starchat.entities.{Term, Terms}
import com.getjenny.starchat.services._
import scalaz.Scalaz._

/**
  * Created by mal on 20/02/2017.
  */

class W2VCosineWordAtomic(arguments: List[String], restrictedArgs: Map[String, String]) extends AbstractAtomic {
  /**
    * Return the normalized w2vcosine similarity of the nearest word
    */

  val word: String = arguments.headOption match {
    case Some(t) => t
    case _ =>
      throw ExceptionAtomic("similar requires an argument")
  }

  override def toString: String = "similar(\"" + word + "\")"

  val termService: TermService.type = TermService

  val indexName: String = restrictedArgs("index_name")

  val (sentenceVector: Vector[Double], reliabilityFactor: Double) =
    TextToVectorsTools.sumOfVectorsFromText(indexName, word)

  val isEvaluateNormalized: Boolean = true
  def evaluate(query: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {
    val textVectors = termService.textToVectors(indexName, query)

    val termsList = textVectors.terms.getOrElse(Terms(terms = List.empty[Term]))
    val distanceList = termsList.terms.map(term => term.vector.getOrElse(Vector.empty[Double]))
      .map(wordVector =>
        if(wordVector.isEmpty || reliabilityFactor === 0.0)
          0.0
        else
          1 - cosineDist(wordVector, sentenceVector)
      )

    val score = if (distanceList.nonEmpty) distanceList.max else 0.0
    Result(score=score)
  }

  // Similarity is normally the cosine itself. The threshold should be at least
  // angle < pi/2 (cosine > 0), but for synonyms let's put cosine > 0.6, i.e. self.evaluate > 0.8
  override val matchThreshold: Double = 0.8
}
