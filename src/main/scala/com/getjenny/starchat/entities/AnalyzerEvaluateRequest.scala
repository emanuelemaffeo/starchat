package com.getjenny.starchat.entities

/**
  * Created by angelo on 07/04/17.
  */

case class AnalyzerEvaluateRequest(analyzer: String,
                                   query: String,
                                   data: Option[AnalyzersData_v4],
                                   stateName: Option[String] = None,
                                   searchAlgorithm: Option[SearchAlgorithm.Value] = Some(SearchAlgorithm.DEFAULT),
                                  )
