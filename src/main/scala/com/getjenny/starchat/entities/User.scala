package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 17/11/17.
  */

import scalaz.Scalaz._

object Permissions extends Enumeration {
  type Permission = Value
  val stream, read, write, admin, unknown = Value
  def value(permission: String): Permissions.Value = values.find(_.toString === permission).getOrElse(unknown)
}

case class User(
                 id: String, /** user id */
                 password: String, /** user password */
                 salt: String, /** salt for password hashing */
                 permissions: Map[
                   String, /** index name */
                   Set[Permissions.Value] /** permissions granted for the index */
                   ]
               )
