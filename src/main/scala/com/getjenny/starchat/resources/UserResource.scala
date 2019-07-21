package com.getjenny.starchat.resources

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 20/12/16.
  */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.{AbstractUserService, UserService}

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait UserResource extends StarChatResource {

  private[this] val userService: AbstractUserService = UserService.service

  def postUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      post {
        authenticateBasicAsync(realm = authRealm,
          authenticator = authenticator.authenticator) { user =>
          authorizeAsync(_ =>
            authenticator.hasPermissions(user, "admin", Permissions.admin)) {
            entity(as[User]) { user_entity =>
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(userService.create(user_entity)) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
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

  def putUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        put {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              entity(as[UserUpdate]) { user_entity =>
                val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(userService.update(id, user_entity)) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => completeResponse(StatusCodes.BadRequest,
                    Option {
                      IndexManagementResponse(message = e.getMessage)
                      ReturnMessageData(code = 101, message = e.getMessage)
                    })
                }
              }
            }
          }
        }
      }
    }
  }

  def deleteUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        delete {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(userService.delete(id)) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    ReturnMessageData(code = 102, message = e.getMessage)
                  })
              }
            }
          }
        }
      }
    }
  }

  def getUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(userService.read(id)) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    ReturnMessageData(code = 103, message = e.getMessage)
                  })
              }
            }
          }
        }
      }
    }
  }

  def genUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user_gen") {
      path(Segment) { id =>
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              entity(as[UserUpdate]) { user_entity =>
                val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(Future {
                  userService.genUser(id, user_entity, authenticator)
                }) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => completeResponse(StatusCodes.BadRequest,
                    Option {
                      ReturnMessageData(code = 104, message = e.getMessage)
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

