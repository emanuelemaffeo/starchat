package com.getjenny.starchat.entities

/**
 * Created by angelo on 07/04/17.
 */

import com.getjenny.analyzer.expressions.AnalyzersData

case class AnalyzerEvaluateResponse(build: Boolean, value: Double, data: Option[AnalyzersData],
                                    buildMessage: String)
