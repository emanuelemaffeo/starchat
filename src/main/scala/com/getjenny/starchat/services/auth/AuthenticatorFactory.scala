package com.getjenny.starchat.services.auth

import com.getjenny.starchat.services._
import scalaz.Scalaz._

case class AuthenticatorClassNotFoundException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

object SupportedAuthImpl extends Enumeration {
  type SupportedAuth = Value
  val basic_http, unknown = Value
  def getValue(authMethod: String): SupportedAuthImpl.Value =
    values.find(_.toString === authMethod).getOrElse(SupportedAuthImpl.unknown)
}

object AuthenticatorFactory {
  def apply(authMethod: SupportedAuthImpl.Value,
            userService: AbstractUserService): AbstractStarChatAuthenticator = {
    authMethod match {
      case SupportedAuthImpl.basic_http =>
        new BasicHttpStarChatAuthenticator(userService)
      case _ =>
        throw AuthenticatorClassNotFoundException("Authenticator not supported: " + authMethod)
    }
  }
}