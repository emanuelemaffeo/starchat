package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 28/02/19.
  */

case class NodeLoadingAllDtStatus(
                              totalIndexes: Long,
                              updatedIndexes: Long,
                              indexes: Map[String, Boolean]
                              )
