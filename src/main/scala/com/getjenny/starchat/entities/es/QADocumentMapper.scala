package com.getjenny.starchat.entities.es

import com.getjenny.starchat.services.QuestionAnswerServiceException

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

object QADocumentMapper {
  def documentFromMap(indexName: String, id: String, source: Map[String, Any]): QADocument = {
    val conversation: String = source.get("conversation") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => throw QuestionAnswerServiceException("Missing conversation ID for " +
        "index:docId(" + indexName + ":" + id + ")")
    }

    val indexInConversation: Int = source.get("index_in_conversation") match {
      case Some(t) => t.asInstanceOf[Int]
      case _ => throw QuestionAnswerServiceException("Missing index in conversation for " +
        "index:docId(" + indexName + ":" + id + ")")
    }

    val status: Option[Int] = source.get("status") match {
      case Some(t) => Some(t.asInstanceOf[Int])
      case _ => Some(0)
    }

    val timestamp: Option[Long] = source.get("timestamp") match {
      case Some(t) => Option {
        t.asInstanceOf[Long]
      }
      case _ => None: Option[Long]
    }

    // begin core data
    val question: Option[String] = source.get("question") match {
      case Some(t) => Some(t.asInstanceOf[String])
      case _ => None
    }

    val questionNegative: Option[List[String]] = source.get("question_negative") match {
      case Some(t) =>
        val res = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
          .asScala.map(_.asScala.get("query")).filter(_.nonEmpty).map(_.get).toList
        Option {
          res
        }
      case _ => None: Option[List[String]]
    }

    val questionScoredTerms: Option[List[(String, Double)]] = source.get("question_scored_terms") match {
      case Some(t) => extractTermList(t)
      case _ => None: Option[List[(String, Double)]]
    }

    val answer: Option[String] = source.get("answer") match {
      case Some(t) => Some(t.asInstanceOf[String])
      case _ => None
    }

    val answerScoredTerms: Option[List[(String, Double)]] = source.get("answer_scored_terms") match {
      case Some(t) => extractTermList(t)
      case _ => None: Option[List[(String, Double)]]
    }

    val topics: Option[String] = source.get("topics") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val verified: Option[Boolean] = source.get("verified") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val done: Option[Boolean] = source.get("done") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val coreDataOut = QADocumentCore(
      question = question,
      questionNegative = questionNegative,
      questionScoredTerms = questionScoredTerms,
      answer = answer,
      answerScoredTerms = answerScoredTerms,
      topics = topics,
      verified = verified,
      done = done
    )

    // begin core data

    // begin annotations
    val dclass: Option[String] = source.get("dclass") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val doctype: Option[Doctypes.Value] = source.get("doctype") match {
      case Some(t) => Some {
        Doctypes.value(t.asInstanceOf[String])
      }
      case _ => Some {
        Doctypes.NORMAL
      }
    }

    val state: Option[String] = source.get("state") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val agent: Option[Agent.Value] = source.get("agent") match {
      case Some(t) => Some(Agent.value(t.asInstanceOf[String]))
      case _ => Some(Agent.STARCHAT)
    }

    val escalated: Option[Escalated.Value] = source.get("escalated") match {
      case Some(t) => Some(Escalated.value(t.asInstanceOf[String]))
      case _ => Some(Escalated.UNSPECIFIED)
    }

    val answered: Option[Answered.Value] = source.get("answered") match {
      case Some(t) => Some(Answered.value(t.asInstanceOf[String]))
      case _ => Some(Answered.ANSWERED)
    }

    val triggered: Option[Triggered.Value] = source.get("triggered") match {
      case Some(t) => Some(Triggered.value(t.asInstanceOf[String]))
      case _ => Some(Triggered.UNSPECIFIED)
    }

    val followup: Option[Followup.Value] = source.get("followup") match {
      case Some(t) => Some(Followup.value(t.asInstanceOf[String]))
      case _ => Some(Followup.UNSPECIFIED)
    }

    val feedbackConv: Option[String] = source.get("feedbackConv") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case _ => None: Option[String]
    }

    val feedbackConvScore: Option[Double] = source.get("feedbackConvScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val algorithmConvScore: Option[Double] = source.get("algorithmConvScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val feedbackAnswerScore: Option[Double] = source.get("feedbackAnswerScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val algorithmAnswerScore: Option[Double] = source.get("algorithmAnswerScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val responseScore: Option[Double] = source.get("responseScore") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case _ => None: Option[Double]
    }

    val start: Option[Boolean] = source.get("start") match {
      case Some(t) => Some(t.asInstanceOf[Boolean])
      case _ => Some(false)
    }

    val annotationsOut = QADocumentAnnotations(
      dclass = dclass,
      doctype = doctype,
      state = state,
      agent = agent,
      escalated = escalated,
      answered = answered,
      triggered = triggered,
      followup = followup,
      feedbackConv = feedbackConv,
      feedbackConvScore = feedbackConvScore,
      algorithmConvScore = algorithmConvScore,
      feedbackAnswerScore = feedbackAnswerScore,
      algorithmAnswerScore = algorithmAnswerScore,
      responseScore = responseScore,
      start = start
    )
    // end annotations

    QADocument(
      id = id,
      conversation = conversation,
      indexInConversation = indexInConversation,
      status = status,
      coreData = Option(coreDataOut),
      annotations = Option(annotationsOut),
      timestamp = timestamp
    )
  }

  private[this] def extractTermList(t: Any): Option[List[(String, Double)]] = {
    Option {
      t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
        .map(pair =>
          (pair.getOrDefault("term", "").asInstanceOf[String],
            pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
        .toList
    }
  }
}
