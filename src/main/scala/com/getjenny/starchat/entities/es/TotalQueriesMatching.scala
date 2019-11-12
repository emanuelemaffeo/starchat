package com.getjenny.starchat.entities.es
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class TotalQueriesMatching(queriesTotalHits: Long)

object TotalQueriesMatchingEntityManager extends ReadEntityManager[TotalQueriesMatching] {
  override def fromSearchResponse(response: SearchResponse): List[TotalQueriesMatching] = {
    response.getHits
      .getHits.toList
      .map(_.getInnerHits.get("queries").getTotalHits.value)
      .map(TotalQueriesMatching)
  }

  override def fromGetResponse(response: List[GetResponse]): List[TotalQueriesMatching] = {
    throw new NotImplementedException()
  }
}
