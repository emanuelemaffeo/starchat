package com.getjenny.starchat.services.esclient

import akka.event.{Logging, LoggingAdapter}
import com.getjenny.starchat.SCActorSystem
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.{SearchResponse, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.sort.SortBuilder
import scalaz.Scalaz._

import scala.collection.JavaConverters._

//private constructor - only the factory object can create an instance,
// in order to be sure that the index name and instance passed as parameters are formatted correctly,
class IndexLanguageCrud private(override val client: ElasticClient, override val index: String, instance: String) extends EsCrudBase(client, index){

  private[this] val log: LoggingAdapter = Logging(SCActorSystem.system, this.getClass.getCanonicalName)


  private[this] val instanceFieldName = "instance"

  override def read(queryBuilder: QueryBuilder,
           from: Option[Int] = None,
           sort: List[SortBuilder[_]] = List.empty,
           maxItems: Option[Int] = None,
           searchType: SearchType = SearchType.DEFAULT,
           aggregation: List[AggregationBuilder] = List.empty,
           requestCache: Option[Boolean] = None,
           minScore: Option[Float] = None,
           scroll: Boolean = false,
           scrollTime: Long = 60000,
           version: Option[Boolean] = None,
           fetchSource: Option[Array[String]] = None): SearchResponse = {

    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    super.read(finalQuery, from, sort, maxItems, searchType, aggregation, requestCache, minScore, scroll,
      scrollTime, version, fetchSource)
  }

  override def create(id: String, builder: XContentBuilder): IndexResponse = {
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    super.create(id, builder)
  }

  override def bulkCreate(elems: List[(String, XContentBuilder)]): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }
    super.bulkCreate(elems)
  }

  override def update(id: String, builder: XContentBuilder, upsert: Boolean = false): UpdateResponse = {
    require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")

    val readResponse = super.read(id)
    if (readResponse.isExists && !readResponse.isSourceEmpty) {
      val savedInstance = readResponse.getSourceAsMap.asScala.getOrElse(instanceFieldName, "").toString
      if(instance =/= savedInstance) {
        log.error(s"Trying to update instance $instance with id $id owned by $savedInstance")
        throw new IllegalArgumentException(s"Trying to update instance: $instance with id: $id owned by: $savedInstance")
      }
    }

    super.update(id, builder, upsert)
  }

  override def bulkUpdate(elems: List[(String, XContentBuilder)], upsert: Boolean = false): BulkResponse = {
    elems.foreach { case (_, builder) =>
      require(isInstanceEvaluated(builder, instance), "instance field must be present while indexing a new document")
    }

    val readResponse = readAll(elems.map { case (id, _) => id })
    val otherInstancesIds = readResponse.getResponses
      .filterNot(x => x.getResponse.isSourceEmpty)
      .map(x => x.getId -> x.getResponse.getSourceAsMap.get(instanceFieldName).toString)
      .filterNot { case (_, readInstance) => readInstance === instance }
      .toSet

    if (otherInstancesIds.nonEmpty){
      log.error("Trying to update instance {} with id owned by another instance - id list: {}", instance, otherInstancesIds.mkString(";"))
      throw new IllegalArgumentException(s"Trying to update instance: $instance with id previously created by another instance")
    }

    super.bulkUpdate(elems, upsert)
  }

  override def delete(queryBuilder: QueryBuilder): BulkByScrollResponse = {
    val finalQuery = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(instanceFieldName, instance))
      .must(queryBuilder)

    super.delete(finalQuery)
  }

  private[this] def isInstanceEvaluated(builder: XContentBuilder, instance: String): Boolean = {
    val parser = builder.contentType().xContent()
      .createParser(NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, Strings.toString(builder))

    val result = Option(if (parser.nextToken() === XContentParser.Token.START_ARRAY) {
      ObjectPath.eval[String](instanceFieldName, parser.listOrderedMap())
    } else {
      ObjectPath.eval[String](instanceFieldName, parser.mapOrdered())
    })

    result.exists(_ === instance)
  }

}

object IndexLanguageCrud {
  def apply(client: ElasticClient, index: String): IndexLanguageCrud = {
    val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(index, client.indexSuffix)
    val instance = Index.instanceName(index)
    new IndexLanguageCrud(client, esLanguageSpecificIndexName, instance)
  }
}
