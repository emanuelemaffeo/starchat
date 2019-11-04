package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 10/02/17.
 */

case class DTAnalyzerLoad(numOfEntries: Int)

case class DTAnalyzerItem(analyzer: String, build: Boolean, executionOrder: Int, evaluationClass: String)

case class DTAnalyzerMap(analyzerMap: Map[String, DTAnalyzerItem])
