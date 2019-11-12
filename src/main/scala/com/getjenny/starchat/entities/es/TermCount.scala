package com.getjenny.starchat.entities.es

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.metrics.Sum
import scalaz.Scalaz._
import sun.reflect.generics.reflectiveObjects.NotImplementedException

object TermCountFields extends Enumeration {
  type TermCountField = Value
  val question, answer, all, unknown = Value
  def value(termCountField: String): TermCountFields.Value =
    values.find(_.toString === termCountField).getOrElse(unknown)
}

case class TermCount(numDocs: Long, count: Long)

object TermCountEntityManager extends ReadEntityManager[TermCount] {
  override def fromSearchResponse(response: SearchResponse): List[TermCount] = {
    val totalHits = response.getHits.getTotalHits.value
    val aggRes: Sum = response.getAggregations.get("countTerms")

    List(TermCount(numDocs = totalHits, count = aggRes.getValue.toLong))
  }

  override def fromGetResponse(response: List[GetResponse]): List[TermCount] = {
    throw new NotImplementedException()
  }
}
