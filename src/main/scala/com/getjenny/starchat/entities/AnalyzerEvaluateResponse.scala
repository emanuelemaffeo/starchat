package com.getjenny.starchat.entities

/**
  * Created by angelo on 07/04/17.
  */

case class AnalyzerEvaluateResponse(build: Boolean, value: Double, data: Option[AnalyzersData_v4],
                                    build_message: String)
