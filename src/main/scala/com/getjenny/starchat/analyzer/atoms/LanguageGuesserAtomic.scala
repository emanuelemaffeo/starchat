package com.getjenny.starchat.analyzer.atoms

import com.getjenny.analyzer.atoms
import com.getjenny.analyzer.atoms.{AbstractAtomic, ExceptionAtomic}
import com.getjenny.analyzer.expressions.{AnalyzersDataInternal, Result}
import org.apache.tika.langdetect.OptimaizeLangDetector
import org.apache.tika.language.detect.{LanguageDetector, LanguageResult}

class LanguageGuesserAtomic (arguments: List[String], restrictedArgs: Map[String, String]) extends AbstractAtomic {

  /** The first argument is the variable name where to store the guessed language */
  val variableName: String = arguments.headOption match {
    case Some(s) => s
    case _ => throw ExceptionAtomic("language guesser requires an variable name")
  }

  /** The second argument is the minimum threshold of the confidence */
  val minScoreThreshold: Double = arguments.lift(1) match {
    case Some(s) => s.toDouble
    case _ => throw atoms.ExceptionAtomic("language guesser requires a threshold")
  }

  /** Optional languages to match the guessed language */
  val languages: List[String] = arguments.drop(2)

  val isEvaluateNormalized = true

  private[this] val detector: LanguageDetector = new OptimaizeLangDetector().loadModels()

  override def toString: String = "languageGuesser(\"" + variableName + "\", \"" + minScoreThreshold + "\"" +
  (languages match {
    case Nil => ""
    case head :: tail => ", [\"" + tail.foldLeft(head)(_ + "\", \"" + _ ) + "\"]"
  }) + ")"

  def evaluate(query: String, data: AnalyzersDataInternal = AnalyzersDataInternal()): Result = {
    val languageResult: LanguageResult = detector.detect(query)
    val score: Double = if(languageResult.getRawScore < minScoreThreshold) 0.0d else languages match {
      case Nil => 1.0d
      case _ => if(languages.contains(languageResult.getLanguage)) 1.0d else 0.0d
    }

    Result(score, data.copy(
      extractedVariables = data.extractedVariables
        .updated(variableName, languageResult.getLanguage)
      )
    )

  }
}
