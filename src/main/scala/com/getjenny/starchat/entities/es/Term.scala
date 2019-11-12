package com.getjenny.starchat.entities.es

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 11/07/17.
 */

import com.getjenny.starchat.services.TermServiceException
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

case class Term(term: String,
                synonyms: Option[Map[String, Double]] = None,
                antonyms: Option[Map[String, Double]] = None,
                tags: Option[String] = None,
                features: Option[Map[String, String]] = None,
                frequencyBase: Option[Double] = None,
                frequencyStem: Option[Double] = None,
                vector: Option[Vector[Double]] = None,
                score: Option[Double] = None
               )

case class SearchTerm(term: Option[String] = None,
                      synonyms: Option[Map[String, Double]] = None,
                      antonyms: Option[Map[String, Double]] = None,
                      tags: Option[String] = None,
                      features: Option[Map[String, String]] = None,
                      frequencyBase: Option[Double] = None,
                      frequencyStem: Option[Double] = None,
                      vector: Option[Vector[Double]] = None,
                      score: Option[Double] = None
                     )

case class Terms(terms: List[Term])

case class TermsResults(total: Int, maxScore: Float, hits: Terms)

case class TextTerms(
                      text: String,
                      textTermsN: Int,
                      termsFoundN: Int,
                      terms: Terms
                    )

case class TermsInfo(terms: List[Term], maxScore: Option[Float] = None)

class TermEntityManager(outputScore: Boolean = true) extends EsEntityManager[Term, TermsInfo] {
  override def fromSearchResponse(response: SearchResponse): List[TermsInfo] = {
    val documents = response.getHits
      .getHits.toList
      .map { item =>
        val score = if(outputScore){
          Option(item.getScore.toDouble)
        } else None
        extractTerms(item.getSourceAsMap.asScala.toMap, score)
      }

    val maxScore = response.getHits.getMaxScore
    List(TermsInfo(documents, Some(maxScore)))
  }

  private[this] def extractTerms(source: Map[String, Any], score: Option[Double] = None): Term = {
    val term: String = source.get("term") match {
      case Some(t) => t.asInstanceOf[String]
      case None => ""
    }

    val synonyms: Option[Map[String, Double]] = source.get("synonyms") match {
      case Some(t) =>
        val value: String = t.asInstanceOf[String]
        Option {
          payloadStringToMapStringDouble(value)
        }
      case None => None: Option[Map[String, Double]]
    }

    val antonyms: Option[Map[String, Double]] = source.get("antonyms") match {
      case Some(t) =>
        val value: String = t.asInstanceOf[String]
        Option {
          payloadStringToMapStringDouble(value)
        }
      case None => None: Option[Map[String, Double]]
    }

    val tags: Option[String] = source.get("tags") match {
      case Some(t) => Option {
        t.asInstanceOf[String]
      }
      case None => None: Option[String]
    }

    val features: Option[Map[String, String]] = source.get("features") match {
      case Some(t) =>
        val value: String = t.asInstanceOf[String]
        Option {
          payloadStringToMapStringString(value)
        }
      case None => None: Option[Map[String, String]]
    }

    val frequencyBase: Option[Double] = source.get("frequency_base") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case None => None: Option[Double]
    }

    val frequencyStem: Option[Double] = source.get("frequency_stem") match {
      case Some(t) => Option {
        t.asInstanceOf[Double]
      }
      case None => None: Option[Double]
    }

    val vector: Option[Vector[Double]] = source.get("vector") match {
      case Some(t) =>
        val value: String = t.asInstanceOf[String]
        Option {
          payloadStringToDoubleVector(value)
        }
      case None => None: Option[Vector[Double]]
    }

    Term(term = term,
      synonyms = synonyms,
      antonyms = antonyms,
      tags = tags,
      features = features,
      frequencyBase = frequencyBase,
      frequencyStem = frequencyStem,
      vector = vector,
      score = score)
  }

  /** transform a payload string (non sparse vector) to a key, value Map[String, Double]
   *
   * @param payload the payload <value index>|<value>
   * @return a key, value map
   */
  private[this] def payloadStringToMapStringDouble(payload: String): Map[String, Double] = {
    payload.split(" ").map(x => {
      val termTuple = x.split("\\|") match {
        case Array(key, value) => (key, value.toDouble)
        case _ =>
          throw TermServiceException("unable to convert string to string->double map")
      }
      termTuple
    }).toMap
  }

  /** transform a payload string (non sparse vector) to a key, value Map[String, String]
   *
   * @param payload the payload <value index>|<value>
   * @return a key, value map
   */
  private[this] def payloadStringToMapStringString(payload: String): Map[String, String] = {
    payload.split(" ").map(x => {
      val termTuple = x.split("\\|") match {
        case Array(key, value) => (key, value)
        case _ =>
          throw TermServiceException("unable to convert string to string->string map")
      }
      termTuple
    }).toMap
  }

  /** transform a payload string (non sparse vector) to a Double vector
   *
   * @param payload the payload <value index>|<value>
   * @return a double vector
   */
  private[this] def payloadStringToDoubleVector(payload: String): Vector[Double] = {
    payload.split(" ").map(x => {
      val termTuple: Double = x.split("\\|") match {
        case Array(_, value) => value.toDouble
        case _ =>
          throw TermServiceException("unable to convert payload string to double vector")
      }
      termTuple
    }).toVector
  }

  override def fromGetResponse(response: List[GetResponse]): List[TermsInfo] = {
    val documents = response
      .filter(_.isExists)
      .map { x => extractTerms(x.getSourceAsMap.asScala.toMap)
      }
    List(TermsInfo(documents))
  }

  override def toXContentBuilder(entity: Term, instance: String): (String, XContentBuilder) = {
    createTermList(entity, instance)
  }

  private[this] def createTermList(term: Term, instance: String): (String, XContentBuilder) = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("instance", instance)
    builder.field("term", term.term)

    term.synonyms.foreach { x =>
      val indexable: String = payloadMapToString[String, Double](x)
      builder.field("synonyms", indexable)
    }
    term.antonyms.foreach { x =>
      val indexable: String = payloadMapToString[String, Double](x)
      builder.field("antonyms", indexable)
    }
    term.tags.foreach(x => builder.field("tags", x))
    term.features.foreach { x =>
      val indexable: String = payloadMapToString[String, String](x)
      builder.field("features", indexable)
    }
    term.frequencyBase.foreach(x => builder.field("frequency_base", x))
    term.frequencyStem.foreach(x => builder.field("frequency_stem", x))
    term.vector.foreach { x =>
      val indexable_vector: String = payloadVectorToString[Double](x)
      builder.field("vector", indexable_vector)
    }

    builder.endObject()
    createId(instance, term.term) -> builder
  }

  /** transform a Key,Value map to a string payload which can be stored on Elasticsearch
   *
   * @param payload the map of values
   * @tparam T the type of the key
   * @tparam U the type of the value
   * @return a string with the payload <value index>|<value>
   */
  private[this] def payloadMapToString[T, U](payload: Map[T, U]): String = {
    payload.map { case (e1, e2) => e1.toString + "|" + e2.toString }.mkString(" ")
  }

  /** transform a vector of numerical values to a string payload which can be stored on Elasticsearch
   *
   * @param vector a vector of values
   * @tparam T the type of the Vector
   * @return a string with the payload <value index>|<value>
   */
  private[this] def payloadVectorToString[T](vector: Vector[T]): String = {
    vector.zipWithIndex.map { case (term, index) => index.toString + "|" + term.toString }.mkString(" ")
  }
}
