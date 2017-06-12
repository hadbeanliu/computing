package com.recommendengine.compute.computing

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.lib.classification.TagSplit
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.lib.classification.StringSplit

object Word2Vec {
  
  
  def main(args:Array[String]):Unit={
    
    
     val length=args.length
    var i=0;
    val arg=new java.util.HashMap[String,Any]
    while(i<length){
      
      if(args(i).startsWith("-")){
        arg.put(args(i).substring(1), args(i+1))
        i=i+2
      }else i = i +1
      
    }
    
    
    val sconf=new SparkConf().setAppName("test")
    
    val ss=SparkSession.builder().appName("simple").master("local[*]").config("spark.driver.memory", "2G").config(sconf).getOrCreate()
    
    val sc=ss.sparkContext
    
    val conf=ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")
    
    val tool=new StringSplit
    tool.setSpark(sc)
    tool.setConf(conf)
    tool.setArgs(arg)
    
    
    arg.put("category.col", "f:lb")
    arg.put("content.col","p:cnt")
    arg.put(Computing.DATA_SOURCE, "headlines:item_meta_table")
    tool.setArgs(arg)
    
    tool.run
    val table = ss.table("splitTxt")
    table.show(30)
    word2Vec(ss)
    
//    val too2=new FeatureSelect
//    too2.setSpark(sc)
//    too2.setConf(conf)
//    too2.setArgs(arg)
//    too2.run()    
    
  }
  
  def word2Vec(ss:SparkSession):Unit={
    
    val word2Vec=new Word2Vec()
                  .setInputCol("words").setOutputCol("result")
                  .setVectorSize(3).setMinCount(0)
    
    val table = ss.table("splitTxt") 
    val model = word2Vec.fit(table)
    
    
    model.write.overwrite().save("file:///home/hadoop/result/word2Vec-tag")
                  
  }
  
}