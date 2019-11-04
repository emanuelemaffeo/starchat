package com.getjenny.starchat.resources

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 27/06/16.
 */

import akka.http.scaladsl.server.Route
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.{ConversationLogsService, QuestionAnswerService}

trait ConversationLogsResource extends StarChatResource {
  private[this] val questionAnswerService: QuestionAnswerService = ConversationLogsService
  private[this] val routeName: String = "conversation_logs"
  private[this] val qaResource = new QAResource(questionAnswerService, routeName)

  def clTermsCountRoutes: Route = qaResource.termsCountRoutes

  def clDictSizeRoutes: Route = qaResource.dictSizeRoutes

  def clTotalTermsRoutes: Route = qaResource.totalTermsRoutes

  def clQuestionAnswerStreamRoutes: Route = qaResource.questionAnswerStreamRoutes

  def clQuestionAnswerRoutes: Route = qaResource.questionAnswerRoutes

  def clQuestionAnswerSearchRoutes: Route = qaResource.questionAnswerSearchRoutes

  def clUpdateTermsRoutes: Route = qaResource.updateTermsRoutes

  def clCountersCacheSizeRoutes: Route = qaResource.countersCacheSizeRoutes

  def clQuestionAnswerConversationsRoutes: Route = qaResource.questionAnswerConversationsRoutes

  def clQuestionAnswerAnalyticsRoutes: Route = qaResource.questionAnswerAnalyticsRoutes
}

