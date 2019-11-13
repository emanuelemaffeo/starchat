package com.getjenny.starchat.entities.es
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.metrics.Sum
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class TotalTerms(numDocs: Long, answer: Long, question: Long)

object TotalTermsEntityManager extends ReadEntityManager[TotalTerms] {
  override def fromSearchResponse(response: SearchResponse): List[TotalTerms] = {
    val totalHits = response.getHits.getTotalHits.value

    val questionAggRes: Sum = response.getAggregations.get("question_term_count")
    val answerAggRes: Sum = response.getAggregations.get("answer_term_count")

    List(TotalTerms(numDocs = totalHits,
      question = questionAggRes.getValue.toLong,
      answer = answerAggRes.getValue.toLong))
  }

  override def fromGetResponse(response: List[GetResponse]): List[TotalTerms] = {
    throw new NotImplementedException()
  }
}
