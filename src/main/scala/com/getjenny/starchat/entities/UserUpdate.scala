package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/11/17.
 */

case class UserUpdate(
                       id: String, /** user id */
                       password: Option[String], /** user password */
                       salt: Option[String], /** salt for password hashing */
                       permissions: Option[
                         Map[
                           String, /** index name */
                           Set[Permissions.Value] /** permissions granted for the index */
                         ]
                       ]
                     )
