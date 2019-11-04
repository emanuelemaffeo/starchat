package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
 */

case class QADocumentUpdate(
                             id: List[String], /* list of document ids to update (bulk editing) */
                             conversation: Option[String] = None, /* ID of the conversation (multiple q&a may be inside a conversation) */
                             indexInConversation: Option[Int] = None, /* the index of the document in the conversation flow */
                             coreData: Option[QADocumentCore] = None, /* core question answer fields */
                             annotations: Option[QADocumentAnnotations] = None, /* qa and conversation annotations */
                             status: Option[Int] = None, /* tell whether the document is locked for editing or not, useful for
                                              a GUI to avoid concurrent modifications, 0 means no operations pending */
                             timestamp: Option[Long] = None /* indexing timestamp, automatically calculated if not provided */
                           )