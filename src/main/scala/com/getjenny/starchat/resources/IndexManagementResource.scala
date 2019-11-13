package com.getjenny.starchat.resources

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 19/12/16.
 */

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.InstanceRegistryService

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait IndexManagementResource extends StarChatResource {

  private[this] val instanceRegistry = InstanceRegistryService
  private[this] val createOperation = "create"
  private[this] val disableOperation = "disable"

  def postIndexManagementCreateRoutes: Route = handleExceptions(routesExceptionHandler) {
    concat(
      pathPrefix(indexRegex ~ Slash ~ "index_management" ~ Slash ~ s"^($createOperation|$disableOperation)".r) {
        (indexName, operation) =>
          post {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ => authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 10, callTimeout = 30.seconds)
                onCompleteWithBreakerFuture(breaker)(
                  operation match {
                    case `createOperation` => instanceRegistry.addInstance(indexName)
                    case `disableOperation` => instanceRegistry.disableInstance(indexName)
                  }
                ) {handleResponse}
              }
            }
          }
      }, pathPrefix(indexRegex ~ Slash ~ "index_management") { indexName =>
        delete {
          authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ => authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 10, callTimeout = 30.seconds)
              onCompleteWithBreakerFuture(breaker)(instanceRegistry.markDeleteInstance(indexName)){handleResponse}
            }
          }
        } ~
        get {
          authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ => authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 10, callTimeout = 30.seconds)
              onCompleteWithBreakerFuture(breaker)(instanceRegistry.checkInstance(indexName)) {handleResponse}
            }
          }
        }
      })
  }

  private[this] def handleResponse[T: ToEntityMarshaller](response: Try[T]): Route = response match {
    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {t})
    case Failure(e) => completeResponse(StatusCodes.BadRequest, Option {IndexManagementResponse(message = e.getMessage)})
  }
}

