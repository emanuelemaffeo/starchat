package com.getjenny.starchat

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 15/03/17.
 */

import akka.actor.ActorSystem

object SCActorSystem {
  val system = ActorSystem("starchat-actor-system")
}
