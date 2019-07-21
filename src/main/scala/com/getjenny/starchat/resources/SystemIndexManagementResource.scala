package com.getjenny.starchat.resources

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 14/11/16.
  */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.SystemIndexManagementService

import scala.util.{Failure, Success}

trait SystemIndexManagementResource extends StarChatResource {

  private[this] val systemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService

  def systemGetIndexesRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("system_indices") {
      pathEnd {
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(systemIndexManagementService.indices) {
                case Success(t) =>
                  completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
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

  def systemIndexManagementRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("system_index_management") {
      path(Segment) { operation: String =>
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              operation match {
                case "refresh" =>
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.refresh()) {
                    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                    case Failure(e) => completeResponse(StatusCodes.BadRequest,
                      Option {
                        IndexManagementResponse(message = e.getMessage)
                      })
                  }
                case "create" =>
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.create()) {
                    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                    case Failure(e) => completeResponse(StatusCodes.BadRequest,
                      Option {
                        IndexManagementResponse(message = e.getMessage)
                      })
                  }
                case _ => completeResponse(StatusCodes.BadRequest,
                  Option {
                    IndexManagementResponse(message = "index(system) Operation not supported: " + operation)
                  })
              }
            }
          }
        }
      } ~
        pathEnd {
          get {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(systemIndexManagementService.check()) {
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
          } ~
            delete {
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.remove()) {
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
            } ~
            put {
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.update()) {
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
}


