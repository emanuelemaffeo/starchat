package com.getjenny.starchat.entities

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

case class KBDocument(id: String, /* unique id of the document */
                      conversation: String, /* ID of the conversation (multiple q&a may be inside a conversation) */
                      index_in_conversation: Option[Int], /* the index of the document in the conversation flow */
                      question: String, /* usually what the user of the chat says */
                      question_negative: Option[List[String]], /* list of sentences different to the main question */
                      question_scored_terms: Option[List[(String, Double)]], /* terms list in form {"term": "<term>", "score": 0.2121} */
                      answer: String, /* usually what the operator of the chat says */
                      answer_scored_terms: Option[List[(String, Double)]], /* terms list in form {"term": "<term>", "score": 0.2121} */
                      verified: Boolean = false, /* was the conversation verified by an operator? */
                      topics: Option[String], /* list of topics */
                      dclass: Option[String], /* document classes e.g. group0 group1 etc.*/
                      doctype: Doctypes.Value = Doctypes.normal, /* document type */
                      state: Option[String], /* eventual link to any of the state machine states */
                      status: Int = 0 /* tell whether the document is locked for editing or not, useful for
                                              a GUI to avoid concurrent modifications, 0 means no operations pending */
                   )
