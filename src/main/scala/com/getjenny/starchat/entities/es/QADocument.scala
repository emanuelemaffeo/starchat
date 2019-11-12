package com.getjenny.starchat.entities.es

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

import com.getjenny.analyzer.util.Time
import com.getjenny.starchat.entities.QADocumentUpdate
import com.getjenny.starchat.services.QuestionAnswerServiceException
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import scalaz.Scalaz._

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

object Doctypes extends Enumeration {
  val CANNED,

  /** canned document, indexed but retrieved only under particular circumstances */
  HIDDEN,

  /** hidden document, these are indexed but must not be retrieved,
   * use this type for data used just to improve statistic for data retrieval */
  DECISIONTABLE,

  /** does not contains conversation data, used to redirect the
   * conversation to any state of the decision tree */
  METADATA,

  /** used for metadata e.g. conversation medatada */
  NORMAL = Doctypes.Value

  /** normal document, can be returned to the user as response */
  def value(v: String): Doctypes.Value = values.find(_.toString === v).getOrElse(NORMAL)
}

object Agent extends Enumeration {
  val HUMAN_REPLY,

  /** Answer provided by an agent, must be used when the conversation
   * has been escalated and the platform (a human is carrying on the conversation) and is not possible
   * to discriminate between HUMAN_PICKED and HUMAN_REPLY.
   */
  HUMAN_PICKED,

  /** When an agent chooses and answer suggestion provided by smartLayer */
  STARCHAT,

  /** The answer was provided by the connector */
  CONNECTOR,

  /** the answer was provided by StarChat */
  UNSPECIFIED = Agent.Value

  /** when the information is unset/not applicable */
  def value(v: String): Agent.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
}

object Escalated extends Enumeration {
  val TRANSFERRED,

  /** when the conversation is being transferred to the customer care */
  UNSPECIFIED = Escalated.Value

  /** usually in the middle of a conversation this value is not set,
   * it is known at the end of the conversation or when the user requests to escalate. */
  def value(v: String): Escalated.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
}

object Answered extends Enumeration {
  val ANSWERED,

  /** the answer was provided */
  UNANSWERED,

  /** Question for which no answer was provided i.e. StarChat returns empty list or 404 or the agent didn’t answer */
  UNSPECIFIED = Answered.Value

  /** the information is not applicable */
  def value(v: String): Answered.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
}

object Triggered extends Enumeration {
  val BUTTON,

  /** the answer was triggered by a button */
  ACTION,

  /** the answer was triggered by an action */
  UNSPECIFIED = Triggered.Value

  /** usually this information is not applicable except in the other two cases mentioned before. */
  def value(v: String): Triggered.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
}

object Followup extends Enumeration {
  val FOLLOWUP,

  /** follow up */
  FOLLOWUP_BY_TIME,

  /** follow up dependant on the time of the day */
  UNSPECIFIED = Followup.Value

  /** not applicable */
  def value(v: String): Followup.Value = values.find(_.toString === v).getOrElse(UNSPECIFIED)
}


case class QADocumentCore(
                           question: Option[String] = None, /* usually what the user of the chat says */
                           questionNegative: Option[List[String]] = None, /* list of sentences different to the main question */
                           questionScoredTerms: Option[List[(String, Double)]] = None, /* terms list in form {"term": "<term>", "score": 0.2121} */
                           answer: Option[String] = None, /* usually what the operator of the chat says */
                           answerScoredTerms: Option[List[(String, Double)]] = None, /* terms list in form {"term": "<term>", "score": 0.2121} */
                           topics: Option[String] = None, /* list of topics */
                           verified: Option[Boolean] = None, /* was the conversation verified by an operator? */
                           done: Option[Boolean] = None /* mark the conversation as done, this field is expected to set once for each conversation */
                         )

case class QADocumentAnnotations(
                                  dclass: Option[String] = None, /* document classes e.g. group0 group1 etc.*/
                                  doctype: Option[Doctypes.Value] = None, /* document type */
                                  state: Option[String] = None, /* eventual link to any of the state machine states or starchat state for logs */
                                  agent: Option[Agent.Value] = None,
                                  escalated: Option[Escalated.Value] = None,
                                  answered: Option[Answered.Value] = None,
                                  triggered: Option[Triggered.Value] = None,
                                  followup: Option[Followup.Value] = None,
                                  feedbackConv: Option[String] = None, /* A feedback provided by the user to the conversation */
                                  feedbackConvScore: Option[Double] = None, /* a field to store the score provided by the user to the conversation */
                                  algorithmConvScore: Option[Double] = None, /* a field to store the score calculated by an algorithm related to the conversation i.e. a sentiment
analysis tool (for future use) */
                                  feedbackAnswerScore: Option[Double] = None, /* description: a field to store the score provided by the user for the answer */
                                  algorithmAnswerScore: Option[Double] = None, /* a field to store the score calculated by an algorithm related to the answer i.e. a sentiment
analysis tool (for future use) */
                                  responseScore: Option[Double] = None, /* score of the response e.g. Starchat response score */
                                  start: Option[Boolean] = None /* event determined when a start state is loaded */
                                )

trait QADocumentBase {
  val id: String /* unique id of the document */
  val coreData: Option[QADocumentCore] /* core question answer fields */
  val annotations: Option[QADocumentAnnotations] /* qa and conversation annotations */
  val status: Option[Int] /* tell whether the document is locked for editing or not, useful for
                          a GUI to avoid concurrent modifications, 0 means no operations pending */
  val timestamp: Option[Long] = None /* indexing timestamp, automatically calculated if not provided */
}

case class QADocument(override val id: String,
                      conversation: String, /* ID of the conversation (multiple q&a may be inside a conversation) */
                      indexInConversation: Int = -1, /* the index of the document in the conversation flow */
                      override val coreData: Option[QADocumentCore] = None,
                      override val annotations: Option[QADocumentAnnotations] = Some(QADocumentAnnotations()),
                      override val status: Option[Int] = Some(0),
                      override val timestamp: Option[Long] = None
                     ) extends QADocumentBase

case class QADocumentUpdateEntity(
                             override val id: String,
                             conversation: Option[String] = None, /* ID of the conversation (multiple q&a may be inside a conversation) */
                             indexInConversation: Option[Int] = None, /* the index of the document in the conversation flow */
                             override val coreData: Option[QADocumentCore] = None,
                             override val annotations: Option[QADocumentAnnotations] = None,
                             override val status: Option[Int] = None,
                             override val timestamp: Option[Long] = None
                           ) extends QADocumentBase

object QADocumentUpdateEntity {
  def fromQADocumentUpdate(document: QADocumentUpdate): List[QADocumentUpdateEntity] = {
    document.id.map(id => QADocumentUpdateEntity(id, document.conversation,
      document.indexInConversation, document.coreData, document.annotations, document.status, document.timestamp))

  }
}

class QaDocumentEntityManager(indexName: String) extends EsEntityManager[QADocumentBase, QADocument] {

  override def fromSearchResponse(response: SearchResponse): List[QADocument] = {
    response.getHits.getHits.toList.map { item =>
      val id: String = item.getId
      val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
      documentFromMap(id, source)
    }
  }

  def documentFromMap(id: String, source: Map[String, Any]): QADocument = {
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
      case Some(t) => Option {
        t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
          .map(pair =>
            (pair.getOrDefault("term", "").asInstanceOf[String],
              pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
          .toList
      }
      case _ => None: Option[List[(String, Double)]]
    }

    val answer: Option[String] = source.get("answer") match {
      case Some(t) => Some(t.asInstanceOf[String])
      case _ => None
    }

    val answerScoredTerms: Option[List[(String, Double)]] = source.get("answer_scored_terms") match {
      case Some(t) => Option {
        t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]].asScala
          .map(pair =>
            (pair.getOrDefault("term", "").asInstanceOf[String],
              pair.getOrDefault("score", 0.0).asInstanceOf[Double]))
          .toList
      }
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
      id = extractId(id),
      conversation = conversation,
      indexInConversation = indexInConversation,
      status = status,
      coreData = Option(coreDataOut),
      annotations = Option(annotationsOut),
      timestamp = timestamp
    )
  }


  override def fromGetResponse(response: List[GetResponse]): List[QADocument] = {
    response
      .filter(p => p.isExists)
      .map { item: GetResponse =>
        val id: String = item.getId
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        documentFromMap(id, source)
      }
  }

  override def toXContentBuilder(entity: QADocumentBase, instance: String): (String, XContentBuilder) = entity match {
    case d: QADocument => createBuilder(d, instance)
    case d: QADocumentUpdateEntity => updateBuilder(d, instance)
  }

  private[this] def updateBuilder(document: QADocumentUpdateEntity, instance: String): (String, XContentBuilder) = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("instance", instance)

    document.conversation match {
      case Some(t) => builder.field("conversation", t)
      case _ => ;
    }

    document.indexInConversation match {
      case Some(t) =>
        if (t <= 0) throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
        builder.field("indexInConversation", t)
      case _ => ;
    }

    document.status match {
      case Some(t) => builder.field("status", t)
      case _ => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case _ => ;
    }

    // begin core data
    document.coreData match {
      case Some(coreData) =>
        coreData.question match {
          case Some(t) => builder.field("question", t)
          case _ => ;
        }
        coreData.questionNegative match {
          case Some(t) =>
            val array = builder.startArray("question_negative")
            t.foreach(q => {
              array.startObject().field("query", q).endObject()
            })
            array.endArray()
          case _ => ;
        }
        coreData.questionScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("question_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.answer match {
          case Some(t) => builder.field("answer", t)
          case _ => ;
        }
        coreData.answerScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("answer_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.topics match {
          case Some(t) => builder.field("topics", t)
          case _ => ;
        }
        coreData.verified match {
          case Some(t) => builder.field("verified", t)
          case _ => ;
        }
        coreData.done match {
          case Some(t) => builder.field("done", t)
          case _ => ;
        }
      case _ => QADocumentCore()
    }
    // end core data

    // begin annotations
    document.annotations match {
      case Some(annotations) =>
        annotations.dclass match {
          case Some(t) => builder.field("dclass", t)
          case _ => ;
        }
        builder.field("doctype", annotations.doctype.toString)
        annotations.state match {
          case Some(t) =>
            builder.field("state", t)
          case _ => ;
        }
        annotations.agent match {
          case Some(t) =>
            builder.field("agent", t.toString)
          case _ => ;
        }
        annotations.escalated match {
          case Some(t) =>
            builder.field("escalated", t.toString)
          case _ => ;
        }
        annotations.answered match {
          case Some(t) =>
            builder.field("answered", t.toString)
          case _ => ;
        }
        annotations.triggered match {
          case Some(t) =>
            builder.field("triggered", t.toString)
          case _ => ;
        }
        annotations.followup match {
          case Some(t) =>
            builder.field("followup", t.toString)
          case _ => ;
        }
        annotations.feedbackConv match {
          case Some(t) => builder.field("feedbackConv", t)
          case _ => ;
        }
        annotations.feedbackConvScore match {
          case Some(t) => builder.field("feedbackConvScore", t)
          case _ => ;
        }
        annotations.algorithmConvScore match {
          case Some(t) => builder.field("algorithmConvScore", t)
          case _ => ;
        }
        annotations.feedbackAnswerScore match {
          case Some(t) => builder.field("feedbackAnswerScore", t)
          case _ => ;
        }
        annotations.algorithmAnswerScore match {
          case Some(t) => builder.field("algorithmAnswerScore", t)
          case _ => ;
        }
        annotations.responseScore match {
          case Some(t) => builder.field("responseScore", t)
          case _ => ;
        }
        annotations.start match {
          case Some(t) => builder.field("start", t)
          case _ => ;
        }
      case _ => ;
    }
    // end annotations

    builder.endObject()
    createId(instance, document.id) -> builder
  }

  def createBuilder(document: QADocument, instance: String): (String, XContentBuilder) = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)
    builder.field("instance", instance)
    builder.field("conversation", document.conversation)

    if (document.indexInConversation <= 0) throw QuestionAnswerServiceException("indexInConversation cannot be < 1")
    builder.field("index_in_conversation", document.indexInConversation)

    document.status match {
      case Some(t) => builder.field("status", t)
      case _ => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case _ => builder.field("timestamp", Time.timestampMillis)
    }

    // begin core data
    document.coreData match {
      case Some(coreData) =>
        coreData.question match {
          case Some("") => ;
          case Some(t) =>
            builder.field("question", t)
          case _ => ;
        }
        coreData.questionNegative match {
          case Some(t) =>
            val array = builder.startArray("question_negative")
            t.foreach(q => {
              array.startObject().field("query", q).endObject()
            })
            array.endArray()
          case _ => ;
        }
        coreData.questionScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("question_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.answer match {
          case Some("") => ;
          case Some(t) => builder.field("answer", t)
          case _ => ;
        }
        coreData.answerScoredTerms match {
          case Some(t) =>
            val array = builder.startArray("answer_scored_terms")
            t.foreach { case (term, score) =>
              array.startObject().field("term", term)
                .field("score", score).endObject()
            }
            array.endArray()
          case _ => ;
        }
        coreData.topics match {
          case Some(t) => builder.field("topics", t)
          case _ => ;
        }
        coreData.verified match {
          case Some(t) => builder.field("verified", t)
          case _ => builder.field("verified", false)

        }
        coreData.done match {
          case Some(t) => builder.field("done", t)
          case _ => builder.field("done", false)
        }
      case _ => QADocumentCore()
    }
    // end core data

    // begin annotations
    document.annotations match {
      case Some(annotations) =>
        annotations.dclass match {
          case Some(t) => builder.field("dclass", t)
          case _ => ;
        }
        annotations.doctype match {
          case Some(t) => builder.field("doctype", t.toString)
          case _ => builder.field("doctype", "NORMAL");
        }
        annotations.state match {
          case Some(t) => builder.field("state", t)
          case _ => ;
        }
        annotations.agent match {
          case Some(t) => builder.field("agent", t.toString)
          case _ => builder.field("agent", "STARCHAT");
        }
        annotations.escalated match {
          case Some(t) => builder.field("escalated", t.toString)
          case _ => builder.field("escalated", "UNSPECIFIED");
        }
        annotations.answered match {
          case Some(t) => builder.field("answered", t.toString)
          case _ => builder.field("answered", "ANSWERED");
        }
        annotations.triggered match {
          case Some(t) => builder.field("triggered", t.toString)
          case _ => builder.field("triggered", "UNSPECIFIED");
        }
        annotations.followup match {
          case Some(t) => builder.field("followup", t.toString)
          case _ => builder.field("followup", "UNSPECIFIED");
        }
        annotations.feedbackConv match {
          case Some(t) => builder.field("feedbackConv", t)
          case _ => ;
        }
        annotations.feedbackConvScore match {
          case Some(t) => builder.field("feedbackConvScore", t)
          case _ => ;
        }
        annotations.algorithmConvScore match {
          case Some(t) => builder.field("algorithmConvScore", t)
          case _ => ;
        }
        annotations.feedbackAnswerScore match {
          case Some(t) => builder.field("feedbackAnswerScore", t)
          case _ => ;
        }
        annotations.algorithmAnswerScore match {
          case Some(t) => builder.field("algorithmAnswerScore", t)
          case _ => ;
        }
        annotations.responseScore match {
          case Some(t) => builder.field("responseScore", t)
          case _ => ;
        }
        annotations.start match {
          case Some(t) => builder.field("start", t)
          case _ => builder.field("start", false);
        }
      case _ => QADocumentAnnotations()
    }
    // end annotations

    builder.endObject()

    createId(instance, document.id) -> builder
  }
}