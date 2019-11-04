package com.getjenny.starchat.resources

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import com.getjenny.starchat.entities.{CreateLanguageIndexRequest, IndexManagementResponse, Permissions}
import com.getjenny.starchat.routing.{StarChatCircuitBreaker, StarChatResource}
import com.getjenny.starchat.services.LangaugeIndexManagementService

import scala.util.{Failure, Success}

trait LanguageIndexManagementResource extends StarChatResource {
  private[this] val languageIndexManagementService: LangaugeIndexManagementService.type = LangaugeIndexManagementService
  private val LanguageIndexManagement = "language_index_management"

  def languageIndexManagement: Route = handleExceptions(routesExceptionHandler) {
    concat(
      path(LanguageIndexManagement) {
        pathEnd {
          post {
            authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ => authenticator.hasPermissions(user, "admin", Permissions.write)) {
                entity(as[CreateLanguageIndexRequest]) { createLanguageIndexRequest =>
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreakerFuture(breaker)(languageIndexManagementService.create(createLanguageIndexRequest.languageList)) {
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
          } ~
            get {
              parameters('index_name, 'indexSuffix.as[String].?) { (languageIndex, indexSuffix) =>
                authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
                  authorizeAsync(_ =>
                    authenticator.hasPermissions(user, languageIndex, Permissions.read)) {
                    val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreakerFuture(breaker)(
                      languageIndexManagementService.check(indexName = languageIndex, indexSuffix = indexSuffix)
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
            } ~
            delete {
              parameters('index_name, 'indexSuffix.as[String].?) { (languageIndex, indexSuffix) =>
                authenticateBasicAsync(realm = authRealm,
                  authenticator = authenticator.authenticator) { user =>
                  authorizeAsync(_ => authenticator.hasPermissions(user, languageIndex, Permissions.admin)) {
                    val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreakerFuture(breaker)(
                      languageIndexManagementService.remove(indexName = languageIndex, indexSuffix = indexSuffix)
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
      },
      pathPrefix(LanguageIndexManagement ~ Slash ~ """(open|close)""".r) { operation =>
        pathEnd {
          post {
            parameters('index_name, 'indexSuffix.as[String].?) { (languageIndex, indexSuffix) =>
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, languageIndex, Permissions.write)) {
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreakerFuture(breaker)(
                    languageIndexManagementService.openClose(indexName = languageIndex, indexSuffix = indexSuffix, operation = operation)
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
      },
      pathPrefix(LanguageIndexManagement ~ Slash ~ """(mappings|settings)""".r) { mappingOrSettings =>
        pathEnd {
          put {
            parameters('index_name, 'indexSuffix.as[String].?) { (languageIndex, indexSuffix) =>
              authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, languageIndex, Permissions.admin)) {
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreakerFuture(breaker)(
                    mappingOrSettings match {
                      case "mappings" => languageIndexManagementService.updateMappings(indexName = languageIndex, indexSuffix = indexSuffix)
                      case "settings" => languageIndexManagementService.updateSettings(indexName = languageIndex, indexSuffix = indexSuffix)
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
      },
      pathPrefix(LanguageIndexManagement ~ Slash ~ "refresh") {
        pathEnd {
          post {
            parameters('index_name, 'indexSuffix.as[String].?) { (languageIndex, indexSuffix) =>
              authenticateBasicAsync(realm = authRealm, authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, languageIndex, Permissions.write)) {
                  val breaker: CircuitBreaker = StarChatCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreakerFuture(breaker)(languageIndexManagementService
                    .refresh(indexName = languageIndex, indexSuffix = indexSuffix)) {
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
    )
  }

}
