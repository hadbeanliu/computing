package com.recommendengine.compute.utils

import java.io.Serializable
import com.chenlb.mmseg4j.example.Complex
import com.huaban.analysis.jieba.JiebaSegmenter

object TextSplit extends Serializable{
  
    
    private val seg=new JiebaSegmenter
    
    
    import collection.JavaConversions._
    def process(text:String):String={
      seg.sentenceProcess(text).mkString(",")
    }
//    private val seg=new Complex
////    System.setProperty("mmseg.dic.path","");
//    def process(txt:String)={
//     
//      seg.segWords(txt, ",")
//    }
    
}