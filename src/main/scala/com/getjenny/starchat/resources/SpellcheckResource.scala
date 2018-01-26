package com.getjenny.starchat.resources

/**
  * Created by angelo on 21/04/17.
  */

import akka.http.scaladsl.server.Route
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.SpellcheckService
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import scala.util.{Failure, Success}

trait SpellcheckResource extends MyResource {

  def spellcheckRoutes: Route =
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ "spellcheck") { index_name =>
      val spellcheckService = SpellcheckService
      pathPrefix("terms") {
        pathEnd {
          post {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, index_name, Permissions.read)) {
                entity(as[SpellcheckTermsRequest]) { request =>
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(spellcheckService.termsSuggester(index_name, request)) {
                    case Success(t) =>
                      completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                    case Failure(e) =>
                      log.error("index(" + index_name + ") route=spellcheckRoutes method=POST: " + e.getMessage)
                      completeResponse(StatusCodes.BadRequest,
                        Option {
                          ReturnMessageData(code = 100, message = e.getMessage)
                        })
                  }
                }
              }
            }
          }
        }
      }
    }
}
