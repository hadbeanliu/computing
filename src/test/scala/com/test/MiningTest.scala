package com.test

import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.utils.TextRankKeyWordExtor
import com.recommendengine.compute.conf.ComputingConfiguration
import org.apache.spark.ml.feature.IDFModel
import com.recommendengine.compute.utils.TextSplit
import scala.io.Source

object MiningTest {
  
  
  def main(args:Array[String]):Unit={
    
        var sb = new StringBuffer()
//    sb.append("北京队翻译郭维盛近日参加了")
    Source.fromFile("/home/hadoop/train/test").getLines().foreach { x => sb.append(x).append("\n") }

    
    val ss = SparkSession.builder().master("local[*]").getOrCreate()
    val bsCode =  "headlines_user-analys"
    val conf = ComputingConfiguration.create()
    val extractor=new TextRankKeyWordExtor


     val idfmodel=IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName)
    
    val idf = idfmodel.idf
    val words=TextSplit.process(sb.toString())
    val kwWithTextRank=extractor.getKeyWordWithScore(words)
    
    val hashingTf=new org.apache.spark.mllib.feature.HashingTF(262144)
    val tf=hashingTf.transform(words)
    
    val complex=kwWithTextRank.map{
      w=>
        val index=hashingTf.indexOf(w._1)
        val v=w._2*tf(index)*idf(index)
      (w._1,v)
    }
    println("keyword only TextRank:====")
    kwWithTextRank.take(10).foreach(println)
    
    println("keyword only withTFIDF:====")
    complex.sortWith((a,b)=>a._2>b._2).take(20).foreach(println)
    
    println("keyword only withIDF:====")
    kwWithTextRank.map{
      w=>
        val index=hashingTf.indexOf(w._1)
        val v=w._2*idf(index)
      (w._1,v)
    }.sortWith((a,b)=>a._2>b._2).take(20).foreach(println)
    
    println("keyword only withTF:====")
    kwWithTextRank.map{
      w=>
        val index=hashingTf.indexOf(w._1)
        val v=tf(index)*idf(index)
      (w._1,v)
    }.sortWith((a,b)=>a._2>b._2).take(20).foreach(println)
    println("keyword only TFIDF:====")
    kwWithTextRank.map{
      w=>
        val index=hashingTf.indexOf(w._1)
        val v=tf(index)
      (w._1,v)
    }.sortWith((a,b)=>a._2>b._2).take(20).foreach(println)
  }
  
}