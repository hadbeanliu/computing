package com.recommendengine.compute.utils

import java.io.Serializable
import com.chenlb.mmseg4j.example.Complex
import com.huaban.analysis.jieba.JiebaSegmenter
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory
import java.io.StringReader
import org.lionsoul.jcseg.tokenizer.core.IWord
import scala.collection.mutable.MutableList
import java.io.File
import java.io.PrintWriter
import org.lionsoul.jcseg.tokenizer.core.ILexicon
import scala.io.Source
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.lionsoul.jcseg.tokenizer.NLPSeg

object TextSplit extends Serializable {

  //    private val seg=new JiebaSegmenter

  import collection.JavaConversions._
  //    def process(text:String):String={
  //      seg.sentenceProcess(text).mkString(",")
  //    }

  //    private val seg=new Complex
  ////    System.setProperty("mmseg.dic.path","");
  //    def process(txt:String)={
  //     
  //      seg.segWords(txt, ",")
  //    }

  private val config = new JcsegTaskConfig(true)
  config.EN_SECOND_SEG = false
  private val dic = DictionaryFactory.createSingletonDictionary(config)

  def process(txt: String): Array[String] = {
    val jcseg = SegmentFactory.createJcseg(JcsegTaskConfig.NLP_MODE, config, dic)

    val result = new StringBuffer()

    var word: IWord = null;
    jcseg.reset(new StringReader(txt))

    try {
      while ({
        word = jcseg.next()
        word != null
      }) {
       
        val speech = word.getPartSpeech()
        if (speech != null){
          if (speech(0).startsWith("n") || speech(0).equals("en") && word.getValue.length() > 1) {
            result.append(word.getValue).append(",")
          }
          }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        println(txt)
        throw e
      }
    }
    result.toString().split(",")
  }

  private def filter(word: IWord): Boolean = {
    if (word == null)
      return false
    val speech = word.getPartSpeech()
    if (speech == null)
      false
    else if (speech(0).startsWith("n") || speech(0).equals("en") && word.getValue.length() > 1) {

      true
    } else
      false

  }

  def main(args: Array[String]): Unit = {
    var sb = new StringBuffer()
    sb.append("上个月《建军大业》曝光定妆照时，有网友犀利评论说看着某些演员")
    ////    Source.fromFile("/home/hadoop/train/test").getLines().foreach { x => sb.append(x).append("\n") }
    //    val arr = Array(1, 2, 3, 4)
    //    println(arr.length)x
    process(sb.toString()).foreach { println }
    
//    val word=dic.get(ILexicon.CJK_WORD, "北京")
//    
//    println(word.getEntity,word.getFrequency,word.getType,word.getValue)
        println(dic.get(ILexicon.CJK_WORD, "零件").getPartSpeech()(0))

  }

}
