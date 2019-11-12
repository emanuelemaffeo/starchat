package com.getjenny.starchat.entities.es

import java.util

import com.getjenny.starchat.utils.Base64
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.search.{SearchHit, SearchHits}

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

trait DTDocument{
  val state: String
}

case class DTDocumentCreate(override val state: String,
                            executionOrder: Int,
                            maxStateCount: Int,
                            analyzer: String,
                            queries: List[String],
                            bubble: String,
                            action: String,
                            actionInput: Map[String, String],
                            stateData: Map[String, String],
                            successValue: String,
                            failureValue: String,
                            evaluationClass: Option[String] = Some("default"),
                            version: Option[Long] = Some(0L)
                     ) extends DTDocument

case class DTDocumentUpdate(override val state: String,
                            executionOrder: Option[Int],
                            maxStateCount: Option[Int],
                            analyzer: Option[String],
                            queries: Option[List[String]],
                            bubble: Option[String],
                            action: Option[String],
                            actionInput: Option[Map[String, String]],
                            stateData: Option[Map[String, String]],
                            successValue: Option[String],
                            failureValue: Option[String],
                            evaluationClass: Option[String]
                           ) extends DTDocument

case class SearchDTDocument(score: Float, document: DTDocumentCreate)

case class SearchDTDocumentsResults(total: Int, maxScore: Float, hits: List[SearchDTDocument])

case class SearchDTDocumentsAndNgrams(documents: SearchDTDocument, ngrams: List[List[String]])

class SearchDTDocumentEntityManager(extractor: (Map[String, SearchHits], Map[String, Any]) => (List[String], List[List[String]])) extends EsEntityManager[DTDocument, SearchDTDocumentsAndNgrams] {
  override def fromSearchResponse(response: SearchResponse): List[SearchDTDocumentsAndNgrams] = {
    response.getHits.getHits.toList.map { item: SearchHit =>
      val version: Option[Long] = Some(item.getVersion)
      val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
      val innerHit = Option(item.getInnerHits).map(_.asScala.toMap).getOrElse(Map.empty)
      val (searchDocument, ngrams) = createDtDocument(item.getId, version, source, item.getScore, innerHit, extractor)
      SearchDTDocumentsAndNgrams(searchDocument, ngrams)
    }
  }

  private[this] def createDtDocument(id: String, version: Option[Long], source: Map[String, Any], score: Float = 0f,
                                     innerHit: Map[String, SearchHits] = Map.empty,
                                     queryFunction: (Map[String, SearchHits], Map[String, Any]) => (List[String], List[List[String]])): (SearchDTDocument, List[List[String]]) = {
    val executionOrder: Int = source.get("execution_order") match {
      case Some(t) => t.asInstanceOf[Int]
      case None => 0
    }

    val maxStateCount: Int = source.get("max_state_count") match {
      case Some(t) => t.asInstanceOf[Int]
      case None => 0
    }

    val analyzer: String = source.get("analyzer") match {
      case Some(t) => Base64.decode(t.asInstanceOf[String])
      case None => ""
    }

    val (queries: List[String], ngram: List[List[String]]) = queryFunction(innerHit, source)

    val bubble: String = source.get("bubble") match {
      case Some(t) => t.asInstanceOf[String]
      case None => ""
    }

    val action: String = source.get("action") match {
      case Some(t) => t.asInstanceOf[String]
      case None => ""
    }

    val actionInput: Map[String, String] = source.get("action_input") match {
      case Some(null) => Map[String, String]()
      case Some(t) => t.asInstanceOf[util.HashMap[String, String]].asScala.toMap
      case None => Map[String, String]()
    }

    val stateData: Map[String, String] = source.get("state_data") match {
      case Some(null) => Map[String, String]()
      case Some(t) => t.asInstanceOf[util.HashMap[String, String]].asScala.toMap
      case None => Map[String, String]()
    }

    val successValue: String = source.get("success_value") match {
      case Some(t) => t.asInstanceOf[String]
      case None => ""
    }

    val failureValue: String = source.get("failure_value") match {
      case Some(t) => t.asInstanceOf[String]
      case None => ""
    }

    val evaluationClass: String = source.get("evaluation_class") match {
      case Some(t) => t.asInstanceOf[String]
      case None => "default"
    }

    val state = extractId(id)

    val document: DTDocumentCreate = DTDocumentCreate(state = state, executionOrder = executionOrder,
      maxStateCount = maxStateCount,
      analyzer = analyzer, queries = queries, bubble = bubble,
      action = action, actionInput = actionInput, stateData = stateData,
      successValue = successValue, failureValue = failureValue,
      evaluationClass = Some(evaluationClass), version = version
    )

    val searchDocument: SearchDTDocument = SearchDTDocument(score = score, document = document)
    (searchDocument, ngram)
  }


  override def fromGetResponse(response: List[GetResponse]): List[SearchDTDocumentsAndNgrams] = {
    response
      .filter(p => p.isExists)
      .map { item =>
        val version: Option[Long] = Some(item.getVersion)
        val source: Map[String, Any] = item.getSource.asScala.toMap
        val (searchDocument, _) = createDtDocument(item.getId, version, source, queryFunction = extractor)
        SearchDTDocumentsAndNgrams(searchDocument, List.empty)
      }
  }

  override def toXContentBuilder(entity: DTDocument, instance: String): (String, XContentBuilder) = entity match {
    case document: DTDocumentCreate => createBuilder(document, instance)
    case document: DTDocumentUpdate => updateBuilder(document, instance)
  }

  private[this] def createBuilder(document: DTDocumentCreate, instance: String): (String, XContentBuilder) = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("instance", instance)
    builder.field("state", document.state)
    builder.field("execution_order", document.executionOrder)
    builder.field("max_state_count", document.maxStateCount)
    builder.field("analyzer", Base64.encode(document.analyzer))

    val array = builder.startArray("queries")
    document.queries.foreach(q => {
      array.startObject().field("query", q).endObject()
    })
    array.endArray()

    builder.field("bubble", document.bubble)
    builder.field("action", document.action)

    val actionInputBuilder: XContentBuilder = builder.startObject("action_input")
    for ((k, v) <- document.actionInput) actionInputBuilder.field(k, v)
    actionInputBuilder.endObject()

    val stateDataBuilder: XContentBuilder = builder.startObject("state_data")
    for ((k, v) <- document.stateData) stateDataBuilder.field(k, v)
    stateDataBuilder.endObject()

    builder.field("success_value", document.successValue)
    builder.field("failure_value", document.failureValue)
    val evaluationClass = document.evaluationClass match {
      case Some(t) => t
      case _ => "default"
    }
    builder.field("evaluation_class", evaluationClass)
    builder.endObject()

    createId(instance, document.state) -> builder
  }

  private[this] def updateBuilder(document: DTDocumentUpdate, instance: String): (String, XContentBuilder) = {
    val builder: XContentBuilder = jsonBuilder().startObject()

    builder.field("instance", instance)

    document.analyzer.foreach(x => builder.field("analyzer", Base64.encode(x)))
    document.executionOrder.foreach(x => builder.field("execution_order", x))
    document.maxStateCount.foreach(x => builder.field("max_state_count", x))
    document.queries.foreach { queryList =>
      val array = builder.startArray("queries")
      queryList.foreach(q => {
        array.startObject().field("query", q).endObject()
      })
      array.endArray()
    }
    document.bubble.foreach(x => builder.field("bubble", x))
    document.action.foreach(x => builder.field("action", x))
    document.actionInput.foreach { x =>
      if (x.nonEmpty) {
        val actionInputBuilder: XContentBuilder = builder.startObject("action_input")
        for ((k, v) <- x) actionInputBuilder.field(k, v)
        actionInputBuilder.endObject()
      } else {
        builder.nullField("action_input")
      }
    }
    document.stateData.foreach { x =>
      val stateDataBuilder: XContentBuilder = builder.startObject("state_data")
      for ((k, v) <- x) stateDataBuilder.field(k, v)
      stateDataBuilder.endObject()
    }
    document.successValue.foreach(x => builder.field("success_value", x))
    document.failureValue.foreach(x => builder.field("failure_value", x))
    document.evaluationClass.foreach(x => builder.field("evaluation_class", x))

    builder.endObject()

    createId(instance, document.state) -> builder
  }


}