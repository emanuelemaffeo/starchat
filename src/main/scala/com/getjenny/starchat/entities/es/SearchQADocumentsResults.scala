package com.getjenny.starchat.entities.es

import com.getjenny.starchat.services.QuestionAnswerServiceException
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

case class SearchQADocument(score: Float, document: QADocument)

case class SearchQADocumentsResults(totalHits: Long = 0,
                                    hitsCount: Long = 0,
                                    maxScore: Float = 0.0f,
                                    hits: List[SearchQADocument] = List.empty[SearchQADocument])

class SearchQADocumentEntityManager(indexName: String) extends ReadEntityManager[SearchQADocumentsResults] {
  override def fromSearchResponse(response: SearchResponse): List[SearchQADocumentsResults] = {
    val documents = response.getHits.getHits.toList.map { item: SearchHit =>
        val source: Map[String, Any] = item.getSourceAsMap.asScala.toMap
        val document = QADocumentMapper.documentFromMap(indexName, extractId(item.getId), source)
        SearchQADocument(score = item.getScore, document = document)
      }

    val maxScore: Float = response.getHits.getMaxScore
    val totalHits = response.getHits.getTotalHits.value
    val total: Int = documents.length
    List(SearchQADocumentsResults(totalHits = totalHits, hitsCount = total, maxScore = maxScore, hits = documents))
  }

  override def fromGetResponse(response: List[GetResponse]): List[SearchQADocumentsResults] = {
    val documents = response
      .filter(p => p.isExists)
      .map { item: GetResponse =>
        val source = item.getSource.asScala.toMap
        val document = QADocumentMapper.documentFromMap(indexName, extractId(item.getId), source)
        SearchQADocument(score = .0f, document = document)
      }

    List(SearchQADocumentsResults(totalHits = documents.length, hitsCount = documents.length, hits = documents))
  }
}
