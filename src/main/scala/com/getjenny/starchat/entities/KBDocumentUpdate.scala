package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

case class KBDocumentUpdate(conversation: Option[String],
                            index_in_conversation: Option[Int],
                            question: Option[String],
                            question_negative: Option[List[String]],
                            question_scored_terms: Option[List[(String, Double)]],
                            answer: Option[String],
                            answer_scored_terms: Option[List[(String, Double)]],
                            verified: Option[Boolean],
                            topics: Option[String],
                            dclass: Option[String],
                            doctype: Option[Doctypes.Value],
                            state: Option[String],
                            status: Option[Int]
                   )
