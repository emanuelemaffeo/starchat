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
  private val CreateOperation = "create"
  private val DisableOperation = "disable"
  private val DeleteOperation = "delete"

  def postIndexManagementCreateRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix(indexRegex ~ Slash ~ "index_management" ~ Slash ~ s"^($CreateOperation|$DisableOperation|$DeleteOperation)".r) {
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
                  case CreateOperation => dtReloadService.addInstance(indexName)
                  case DisableOperation => dtReloadService.disableInstance(indexName)
                  case DeleteOperation => dtReloadService.markDeleteInstance(indexName)
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

