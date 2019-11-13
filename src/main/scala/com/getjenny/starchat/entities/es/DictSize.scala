package com.getjenny.starchat.entities.es
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.aggregations.metrics.Cardinality
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class DictSize(numDocs: Long, answer: Long, question: Long, total: Long)

object DictSizeEntityManager extends ReadEntityManager[DictSize] {
  override def fromSearchResponse(response: SearchResponse): List[DictSize] = {
    val questionAggRes: Cardinality = response.getAggregations.get("question_term_count")
    val answerAggRes: Cardinality = response.getAggregations.get("answer_term_count")
    val totalAggRes: Cardinality = response.getAggregations.get("total_term_count")

    List(DictSize(numDocs = response.getHits.getTotalHits.value,
      question = questionAggRes.getValue,
      answer = answerAggRes.getValue,
      total = totalAggRes.getValue))
  }

  override def fromGetResponse(response: List[GetResponse]): List[DictSize] = throw new NotImplementedException()

}
