package com.recommendengine.compute.computing


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.recommendengine.compute.lib.recommendation.BaseRatingRuleRecommend
import org.apache.hadoop.conf.Configuration
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.lib.recommendation.UserGraphAnalysis
import com.recommendengine.compute.lib.classification.WordSplit
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.lib.classification.StringSplit
import com.recommendengine.compute.lib.classification.FeatureSelect
import com.recommendengine.compute.lib.classification.ClassifyModelBuilder
import com.recommendengine.compute.conf.ComputingConfiguration

object NaiveBayes {
  
  
  
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
    
    val ss=SparkSession.builder().appName("simple").master("local[*]").config(sconf).getOrCreate()
    
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
    
//    val too2=new FeatureSelect
//    too2.setSpark(sc)
//    too2.setConf(conf)
//    too2.setArgs(arg)
//    too2.run()
//    
//    val too3=new ClassifyModelBuilder
//    too3.setSpark(sc)
//    too3.setConf(conf)
//    too3.setArgs(arg)
//    too3.run()
   
    
    
    
    
    
  }
  
  
  
  
}