package com.getjenny.starchat.entities.es
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

case class DTStateWordFreqsItem(state: String, wordFreqs: Map[String,Double])

object DtStateWordFreqsEntityManager extends ReadEntityManager[DTStateWordFreqsItem]{
  override def fromSearchResponse(response: SearchResponse): List[DTStateWordFreqsItem] = {
    val pst: ParsedStringTerms = response.getAggregations.get("StatesWordStats")
    pst.getBuckets.asScala.map { stateBucket =>
      //TODO check if is an ID to create as instance|id
        val state = stateBucket.getKeyAsString
        val parsedNested: ParsedNested = stateBucket.getAggregations.get("queries")
        val nQueries: Double = parsedNested.getDocCount
        val parsedStringTerms: ParsedStringTerms = parsedNested.getAggregations.get("queries_children")
        if (nQueries > 0) {
          val wordFreqs = parsedStringTerms.getBuckets.asScala.map {
            bucket => bucket.getKeyAsString -> bucket.getDocCount / nQueries // normalize on nQueries
          }.toMap
          // add to map for each state the histogram wordFreqs
          DTStateWordFreqsItem(state, wordFreqs)
        }
        else
          DTStateWordFreqsItem(state, Map[String, Double]())
    }.toList
  }

  override def fromGetResponse(response: List[GetResponse]): List[DTStateWordFreqsItem] = {
    throw new NotImplementedException()
  }
}
