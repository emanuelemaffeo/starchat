package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 28/01/19.
  */

case class ClusterNode(
                        uuid: String,
                        alive: Boolean,
                        timestamp: Long
                      )

case class ClusterNodes(
                         uuid: String,
                         nodes: List[ClusterNode]
                       )
