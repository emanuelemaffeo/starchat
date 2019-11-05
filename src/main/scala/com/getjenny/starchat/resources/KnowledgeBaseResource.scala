package com.getjenny.starchat.resources

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
 */

import akka.http.scaladsl.server.Route
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.{KnowledgeBaseService, QuestionAnswerService}

trait KnowledgeBaseResource extends StarChatResource {
  private[this] val questionAnswerService: QuestionAnswerService = KnowledgeBaseService
  private[this] val routeName: String = "knowledgebase"
  private[this] val qaResource = new QAResource(questionAnswerService, routeName)

  def kbTermsCountRoutes: Route = qaResource.termsCountRoutes

  def kbDictSizeRoutes: Route = qaResource.dictSizeRoutes

  def kbTotalTermsRoutes: Route = qaResource.totalTermsRoutes

  def kbQuestionAnswerStreamRoutes: Route = qaResource.questionAnswerStreamRoutes

  def kbQuestionAnswerRoutes: Route = qaResource.questionAnswerRoutes

  def kbQuestionAnswerSearchRoutes: Route = qaResource.questionAnswerSearchRoutes

  def kbUpdateTermsRoutes: Route = qaResource.updateTermsRoutes

  def kbCountersCacheSizeRoutes: Route = qaResource.countersCacheSizeRoutes

  def kbQuestionAnswerConversationsRoutes: Route = qaResource.questionAnswerConversationsRoutes

  def kbQuestionAnswerAnalyticsRoutes: Route = qaResource.questionAnswerAnalyticsRoutes
}
