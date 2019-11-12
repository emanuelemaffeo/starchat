package com.getjenny.starchat.entities.es

import java.util

import com.getjenny.starchat.utils.Base64
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

case class DecisionTableItem(state: String,
                             analyzerDeclaration: String,
                             executionOrder: Int = -1,
                             maxStateCounter: Int = -1,
                             evaluationClass: String = "default",
                             version: Long = -1L,
                             queries: List[String] = List.empty[String]
                            )


object DecisionTableEntityManager extends ReadEntityManager[DecisionTableItem] {
  override def fromSearchResponse(response: SearchResponse): List[DecisionTableItem] = {
    response.getHits.getHits.toList.map {
      item: SearchHit =>
        val version: Long = item.getVersion
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        fromSearch(source, item.getId, version)
    }
  }

  private[this] def fromSearch(source: Map[String, Any], id: String, version: Long): DecisionTableItem = {

    val analyzerDeclaration: String = source.get("analyzer") match {
      case Some(t) => Base64.decode(t.asInstanceOf[String])
      case _ => ""
    }

    val executionOrder: Int = source.get("execution_order") match {
      case Some(t) => t.asInstanceOf[Int]
      case _ => 0
    }

    val maxStateCounter: Int = source.get("max_state_counter") match {
      case Some(t) => t.asInstanceOf[Int]
      case _ => 0
    }

    val evaluationClass: String = source.get("evaluation_class") match {
      case Some(t) => t.asInstanceOf[String]
      case _ => "default"
    }

    val queries: List[String] = source.get("queries") match {
      case Some(t) =>
        t.asInstanceOf[util.ArrayList[util.HashMap[String, String]]].asScala.toList
          .map(q_e => q_e.get("query"))
      case None => List[String]()
    }

    val state = extractId(id)

    val decisionTableRuntimeItem: DecisionTableItem =
      DecisionTableItem(state = state,
        analyzerDeclaration = analyzerDeclaration,
        executionOrder = executionOrder,
        maxStateCounter = maxStateCounter,
        queries = queries,
        evaluationClass = evaluationClass,
        version = version)
    decisionTableRuntimeItem
  }

  override def fromGetResponse(response: List[GetResponse]): List[DecisionTableItem] = {
    response
      .filter(p => p.isExists)
      .map { item: GetResponse =>
        val version: Long = item.getVersion
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        fromSearch(source, item.getId, version)
      }
  }

}