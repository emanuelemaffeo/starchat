package com.getjenny.starchat.entities

case class DTStateWordFreqsItem(state: String, wordFreqs: List[DTWordFreqItem]) {
  def getFreqOfWord(word: String): Double = {

    if (wordFreqs.isEmpty)
      0.0d
    else {
      val matchingItemList = wordFreqs.filter(p => p.word == word);
      if (matchingItemList.isEmpty)
        0.0d
      else
        matchingItemList.head.freq
    }
  }

}

