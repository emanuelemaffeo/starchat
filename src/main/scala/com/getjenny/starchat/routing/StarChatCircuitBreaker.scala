package com.getjenny.starchat.routing

import akka.pattern.CircuitBreaker
import com.getjenny.starchat.SCActorSystem

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object StarChatCircuitBreaker {
  implicit def executionContext: ExecutionContext = SCActorSystem.system.dispatcher
  def getCircuitBreaker(maxFailure: Int = 32, callTimeout: FiniteDuration = 20.seconds,
                        resetTimeout: FiniteDuration = 5.seconds): CircuitBreaker = {
    val breaker = new CircuitBreaker(scheduler = SCActorSystem.system.scheduler,
      maxFailures = maxFailure,
      callTimeout = callTimeout,
      resetTimeout = resetTimeout
    )
    breaker
  }
}


