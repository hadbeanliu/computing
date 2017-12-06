package com.recommendengine.compute.computing

import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.lib.classification.{ClassifyModelBuilder, DataExport, FeatureSelect, StringSplit}
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.sql.SparkSession

object DataTransform {

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
    

    val ss=SparkSession.builder().appName("simple").getOrCreate()
    
    val sc=ss.sparkContext
    
    val conf=ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
//    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-test")

//    conf.set("default.model.path", "file:///home/hadoop/result/model")
    
    val tool=new DataExport
    tool.setSpark(sc)
    tool.setConf(conf)

    
    arg.put("category.col", "f:lb")
    arg.put("content.col","p:cnt")
    arg.put(Computing.DATA_SOURCE, "headlines:item_meta_table")
    arg.put("dataSource","hdfs")
    arg.put("dataSource.path","/computing/data")
    arg.put("dataSource.splitRegex","/0003")
    arg.put("label.filter","本地福建福州厦门宁德莆田龙岩南平三明漳州订阅")

    tool.setArgs(arg)
    
    tool.run
    
  }
  
  
  
  
}