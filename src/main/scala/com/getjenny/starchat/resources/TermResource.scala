package com.getjenny.starchat.resources

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 12/03/17.
  */

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import akka.stream.scaladsl.Source
import com.getjenny.starchat.entities._
import com.getjenny.starchat.routing._
import com.getjenny.starchat.services.TermService

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait TermResource extends StarChatResource {

  private[this] val termService: TermService.type = TermService

  def termStreamRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix(
      """^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~
        """stream""" ~ Slash ~
        """term""") { indexName =>
      pathEnd {
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.stream)) {
              extractMethod { method =>
                val entryIterator = termService.allDocuments(indexName)
                val entries: Source[Term, NotUsed] =
                  Source.fromIterator(() => entryIterator)
                complete(entries)
              }
            }
          }
        }
      }
    }
  }

  def termRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ "term") { indexName =>
      path(Segment) { operation: String =>
        post {
          operation match {
            case "index" =>
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, indexName, Permissions.write)) {
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    entity(as[Terms]) { request_data =>
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(Future {
                        termService.indexTerm(indexName, request_data, refresh)
                      }) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") route=termRoutes method=POST function=index : " +
                            e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              ReturnMessageData(code = 100, message = e.getMessage)
                            })
                      }
                    }
                  }
                }
              }
            case "index_default_synonyms" =>
              withoutRequestTimeout {
                authenticateBasicAsync(realm = authRealm,
                  authenticator = authenticator.authenticator) { user =>
                  authorizeAsync(_ =>
                    authenticator.hasPermissions(user, indexName, Permissions.write)) {
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker =
                        StarChatCircuitBreaker.getCircuitBreaker(maxFailure = 5,
                          callTimeout = 120.seconds, resetTimeout = 120.seconds)
                      onCompleteWithBreaker(breaker)(
                        termService.indexDefaultSynonyms(
                          indexName = indexName, refresh = refresh)) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") " +
                            "route=termRoutes method=POST function=index_default_synonyms : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              ReturnMessageData(code = 100, message = e.getMessage)
                            })
                      }
                    }
                  }
                }
              }
            case "get" =>
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, indexName, Permissions.read)) {
                  entity(as[TermIdsRequest]) { request_data =>
                    val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(Future {
                      termService.termsById(indexName, request_data)
                    }) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                      case Failure(e) =>
                        log.error("index(" + indexName + ") route=termRoutes method=POST function=get : " +
                          e.getMessage)
                        completeResponse(StatusCodes.BadRequest,
                          Option {
                            ReturnMessageData(code = 101, message = e.getMessage)
                          })
                    }
                  }
                }
              }
            case _ => completeResponse(StatusCodes.BadRequest,
              Option {
                IndexManagementResponse(message = "index(" + indexName + ") Operation not supported: " +
                  operation)
              })
          }
        }
      } ~
        pathEnd {
          delete {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.write)) {
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[TermIdsRequest]) { request_data =>
                    if (request_data.ids.nonEmpty) {
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(Future{
                        termService.delete(indexName, request_data.ids, refresh)
                      }) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") route=termRoutes method=DELETE : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              ReturnMessageData(code = 102, message = e.getMessage)
                            })
                      }
                    } else {
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(Future {
                        termService.deleteAll(indexName)
                      }) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") route=termRoutes method=DELETE : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              ReturnMessageData(code = 103, message = e.getMessage)
                            })
                      }
                    }
                  }
                }
              }
            }
          } ~
            put {
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, indexName, Permissions.write)) {
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    entity(as[Terms]) { request_data =>
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(termService.updateTermFuture(indexName, request_data, refresh)) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") route=termRoutes method=PUT : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest, Option {
                            IndexManagementResponse(message = e.getMessage)
                          })
                      }
                    }
                  }
                }
              }
            }
        } ~
        path(Segment) { operation: String =>
          get {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ => authenticator.hasPermissions(user, indexName, Permissions.read)) {
                operation match {
                  case "term" =>
                    entity(as[SearchTerm]) { requestData =>
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(
                        termService.searchFuture[SearchTerm](indexName = indexName, query = requestData)
                      ) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                        case Failure(e) =>
                          log.error("index(" + indexName + ") route=termRoutes method=GET function=term : " +
                            e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              IndexManagementResponse(message = e.getMessage)
                            })
                      }
                    }
                  case "text" =>
                    entity(as[String]) { requestData =>
                      val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                      parameters("analyzer".as[String] ? "space_punctuation") { analyzer =>
                        onCompleteWithBreaker(breaker)(
                          termService.searchFuture[String](indexName = indexName, query = requestData, analyzer = analyzer)
                        ) {
                          case Success(t) =>
                            completeResponse(StatusCodes.OK, StatusCodes.BadRequest, t)
                          case Failure(e) =>
                            log.error("index(" + indexName + ") route=termRoutes method=GET function=text : " +
                              e.getMessage)
                            completeResponse(StatusCodes.BadRequest,
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
    }
  }
}
