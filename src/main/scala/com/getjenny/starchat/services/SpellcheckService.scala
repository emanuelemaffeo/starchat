package com.getjenny.starchat.services

/**
 * Created by angelo on 21/04/17.
 */

import com.getjenny.starchat.entities._
import com.getjenny.starchat.services.esclient.KnowledgeBaseElasticClient
import com.getjenny.starchat.utils.Index
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.suggest.SuggestBuilder
import org.elasticsearch.search.suggest.term.{TermSuggestion, TermSuggestionBuilder}

import scala.collection.JavaConverters._
import scala.collection.immutable.List

object SpellcheckService extends AbstractDataService {
  override val elasticClient: KnowledgeBaseElasticClient.type = KnowledgeBaseElasticClient

  //TODO refactor with new dedicated index
  def termsSuggester(indexName: String, request: SpellcheckTermsRequest) : SpellcheckTermsResponse = {
    val esLanguageSpecificIndexName = Index.esLanguageFromIndexName(indexName, elasticClient.indexSuffix)
    val client: RestHighLevelClient = elasticClient.httpClient

    val suggestionBuilder: TermSuggestionBuilder = new TermSuggestionBuilder("question.base")
    suggestionBuilder.maxEdits(request.maxEdit)
      .prefixLength(request.prefixLength)
      .minDocFreq(request.minDocFreq)

    val suggestBuilder: SuggestBuilder = new SuggestBuilder()
    suggestBuilder.setGlobalText(request.text)
      .addSuggestion("suggestions", suggestionBuilder)

    val sourceReq: SearchSourceBuilder = new SearchSourceBuilder()
      .suggest(suggestBuilder)

    val searchReq = new SearchRequest(esLanguageSpecificIndexName)
      .source(sourceReq)

    val searchResponse : SearchResponse = client.search(searchReq, RequestOptions.DEFAULT)

    val termsSuggestions: List[SpellcheckToken] =
      searchResponse.getSuggest.getSuggestion[TermSuggestion]("suggestions")
        .getEntries.asScala.toList.map { suggestions =>
        val item: TermSuggestion.Entry = suggestions
        val text = item.getText.toString
        val offset = item.getOffset
        val length = item.getLength
        val options: List[SpellcheckTokenSuggestions] =
          item.getOptions.asScala.toList.map { suggestion =>
            val option = SpellcheckTokenSuggestions(
              score = suggestion.getScore.toDouble,
              freq = suggestion.getFreq.toDouble,
              text = suggestion.getText.toString
            )
            option
          }
        val spellcheckToken =
          SpellcheckToken(text = text, offset = offset, length = length,
            options = options)
        spellcheckToken
      }

    SpellcheckTermsResponse(tokens = termsSuggestions)
  }
}
