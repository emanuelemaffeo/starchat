package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 28/01/19.
  */


case class ClusterLoadingDtStatusIndex(
                                   index: String,
                                   totalAliveNodes: Long,
                                   upToDateNodes: Long,
                                   updateCompleted: Boolean,
                                   timestamp: Long
                                 )