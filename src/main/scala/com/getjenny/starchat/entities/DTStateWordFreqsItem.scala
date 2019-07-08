package com.getjenny.starchat.entities
import scalaz.Scalaz._
case class DTStateWordFreqsItem(state: String, wordFreqs: List[DTWordFreqItem]) {
  def getFreqOfWord(word: String): Double = {

    if (wordFreqs.isEmpty)
      0.0d
    else {
      wordFreqs.filter(p => p.word === word).headOption match {
        case Some(t) => t.freq
        case _ => 0.0d
      }
    }
  }

}

