package com.getjenny.starchat.resources

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 19/12/16.
 */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.InstanceRegistryService

import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait IndexManagementResource extends StarChatResource {

  private[this] val dtReloadService = InstanceRegistryService
  private[this] val createOperation = "create"
  private[this] val disableOperation = "disable"
  private[this] val deleteOperation = "delete"

  def postIndexManagementCreateRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix(indexRegex ~ Slash ~ "index_management" ~ Slash ~ s"^($createOperation|$disableOperation|$deleteOperation)".r) {
      (indexName, operation) =>
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker
                .getCircuitBreaker(maxFailure = 10, callTimeout = 30.seconds)
              onCompleteWithBreakerFuture(breaker)(
                operation match {
                  case createOperation => dtReloadService.addInstance(indexName)
                  case disableOperation => dtReloadService.disableInstance(indexName)
                  case deleteOperation => dtReloadService.markDeleteInstance(indexName)
                }

              ) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    IndexManagementResponse(message = e.getMessage)
                  })
              }
            }
          }
        }
    }
  }
}

