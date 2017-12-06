package com.recommendengine.compute.utils

import org.lionsoul.jcseg.extractor.impl.TextRankKeywordsExtractor
import org.lionsoul.jcseg.tokenizer.core.ISegment
import org.lionsoul.jcseg.tokenizer.core.IWord
import java.io.StringReader
import java.util.HashMap
import java.util.List
import java.util.ArrayList
import java.util.LinkedList
import collection.JavaConversions._

class TextRankKeyWordExtor(val segs: ISegment) extends TextRankKeywordsExtractor(segs) {

  def this() = this(null)

  val D = 0.85F
 

  private[compute] def extractKeyWordWithScore(words: Array[String],topN:Int=10): Array[(String, Float)] = {

    val winMap = new HashMap[String, List[String]]
    var w: IWord = null
    for {
      w <- words
    } {
      if (!winMap.containsKey(w)) {
        winMap.put(w, new LinkedList[String]());
      }
    }
    for (i <- 0 until words.length) {

      val word = words(i)
      val support = winMap.get(word);

      val sIdx = Math.max(0, i - windowSize);
      val eIdx = Math.min(i + windowSize, words.length - 1);

      for {
        j <- sIdx to eIdx
        if (j != i)
      } {
        support.add(words(j));
      }

    }

    val score = new HashMap[String, Float]();

    for (c <- 0 until 20) {

      for (entry <- winMap.entrySet()) {
        val key = entry.getKey();
        val value = entry.getValue();

        var sigema = 0F;
        for {
          ele <- value
          if (!ele.equals(key) && winMap.get(ele).size() != 0)
        } {
          val size = winMap.get(ele).size();

          var Sy = 0f;
          if (score != null
            && score.containsKey(ele)) {
            Sy = score.get(ele);
          }

          sigema += Sy / size;
        }

        score.put(key, 1 - D + D * sigema);
      }
    }

    score.toArray[(String,Float)].sortWith((x, y) => x._2 > y._2).take(topN)
  }
  
  def getKeyWordWithScore(words: Array[String]): Array[(String, Float)] = {
    
    this.extractKeyWordWithScore(words,100)
  }

  def getKeyWordWithScore(str: String,topN:Int=10): Array[(String, Float)] = {

    
    val words =if (this.segs==null) TextSplit.process(str) else Array[String]()
    extractKeyWordWithScore(words, topN)
  }
}