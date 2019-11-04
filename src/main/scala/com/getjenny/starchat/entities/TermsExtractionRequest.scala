package com.getjenny.starchat.entities

/**
 * Created by Angelo Leto <angelo@getjenny.com> on 20/05/18.
 */

case class TermsExtractionRequest (
                                    text: String,
                                    tokenizer: Option[String] = Some("space_punctuation"),
                                    commonOrSpecificSearchPrior: Option[CommonOrSpecificSearch.Value] = Some(CommonOrSpecificSearch.COMMON),
                                    commonOrSpecificSearchObserved: Option[CommonOrSpecificSearch.Value] = Some(CommonOrSpecificSearch.IDXSPECIFIC),
                                    observedDataSource: Option[ObservedDataSources.Value] = Some(ObservedDataSources.KNOWLEDGEBASE),
                                    fieldsPrior: Option[TermCountFields.Value] = Some(TermCountFields.all),
                                    fieldsObserved: Option[TermCountFields.Value] = Some(TermCountFields.all),
                                    minWordsPerSentence: Option[Int] = Some(10),
                                    pruneTermsThreshold: Option[Int] = Some(100000),
                                    misspellMaxOccurrence: Option[Int] = Some(5),
                                    activePotentialDecay: Option[Int] = Some(10),
                                    activePotential: Option[Boolean] = Some(true),
                                    minSentenceInfoBit: Option[Int] = Some(16),
                                    minKeywordInfo: Option[Int] = Some(8),
                                    totalInfo: Option[Boolean] = Some(false)
                                  )
