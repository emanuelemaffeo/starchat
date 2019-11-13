package com.getjenny.starchat.resources

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 19/12/16.
 */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait RootAPIResource extends StarChatResource {
  def rootAPIsRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("") {
      extractRequest { request =>
        pathEnd {
          get {
            val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 5,
              callTimeout = 2.second)
            onCompleteWithBreakerFuture(breaker)(None) {
              case Success(_) =>
                completeResponse(StatusCodes.OK)
              case Failure(e) =>
                log.error(logTemplate("", "indexName", "RootRoutes", request.method, request.uri), e)
                completeResponse(StatusCodes.BadRequest,
                  Option {
                    ReturnMessageData(code = 100, message = e.getMessage)
                  })
            }
          } ~ post {
            val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 5,
              callTimeout = 2.second)
            onCompleteWithBreakerFuture(breaker)(None) {
              case Success(_) =>
                val buildInfo = com.getjenny.starchat.autogen.utils.BuildInfo.toJson
                completeResponse(StatusCodes.OK, buildInfo)
              case Failure(e) =>
                log.error(logTemplate("", "indexName", "RootRoutes", request.method, request.uri), e)
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



