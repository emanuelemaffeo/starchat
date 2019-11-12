package com.getjenny.starchat.entities.es
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

case class WordFrequenciesInQuery(frequencies: Map[String, Double])

object WordFrequenciesInQueryEntityManager extends ReadEntityManager[WordFrequenciesInQuery] {
  override def fromSearchResponse(response: SearchResponse): List[WordFrequenciesInQuery] = {
    val parsedNested: ParsedNested = response.getAggregations.get("queries")
    val nQueries: Double = parsedNested.getDocCount
    val parsedStringTerms: ParsedStringTerms = parsedNested.getAggregations.get("queries_children")
    val result = if (nQueries > 0) {
      parsedStringTerms.getBuckets.asScala.map {
        bucket => bucket.getKeyAsString -> bucket.getDocCount / nQueries
      }.toMap
    }
    else {
      Map[String, Double]()
    }
    List(WordFrequenciesInQuery(result))
  }

  override def fromGetResponse(response: List[GetResponse]): List[WordFrequenciesInQuery] = ???
}