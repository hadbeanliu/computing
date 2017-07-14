package com.recommendengine.compute.computing


import scala.reflect.internal.Required
import scala.util.Random
import com.google.gson.Gson
import com.recommendengine.compute.utils.TextSplit
import com.recommendengine.compute.api.resource.MiningResource
import org.apache.hadoop.fs.FileSystem
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.File
import com.recommendengine.compute.db.hbase.HbaseServer
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.ml.feature.IDFModel
import scala.collection.mutable.Map
import org.restlet.resource.ClientResource
import scala.io.Source
import java.io.PrintWriter
import scala.collection.mutable.WrappedArray
import org.lionsoul.jcseg.test.JcsegTest
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.ml.feature.StringIndexerModel


object Word2Vec {
  
  
  def main(args: Array[String]): Unit = {
    
    val d=1d
    
    println(d.toInt)
    
    val sconf=new SparkConf().setAppName("test")
    
    val ss=SparkSession.builder().appName("simple").master("local[*]").config(sconf).getOrCreate()
    
    val sc=ss.sparkContext
//    
    val conf=ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")
//    conf.set("default.model.path", "file:///home/hadoop/result/model")
//    val hashingTF=new HashingTF
//    println("科技",hashingTF.indexOf("科技"))
//    println("体育",hashingTF.indexOf("体育"))
//    println("国际",hashingTF.indexOf("国际"))
//    
    
   

    import ss.implicits._
    
    
    val toHash = new HashingTF(262144)
//    
    val data= sc.wholeTextFiles("file:///home/hadoop/result/words-with-split").map(f=>(f._1.substring(f._1.lastIndexOf("/")+1),f._2.split("\n").map {
      x =>{
      val split =x.split(" ")
      try{
      if(split.size == 2) (split(0),split(1).toInt) else (x,1) 
      }catch {
        case e:NumberFormatException =>(x,1) 
      }
    }
    
    }))
    
        val bsCode="headlines_user-analys"
    val model = NaiveBayesModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + NaiveBayesModel.getClass.getSimpleName).asInstanceOf[NaiveBayesModel]
    val theta= model.theta
    val stringIndex = StringIndexerModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + StringIndexerModel.getClass.getSimpleName)
    val idfModel=IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName)
    val idf =idfModel.idf

    
//    
    val labels= data.map(_._1).collect().map { x => (1,x) }
    val labelMaps=stringIndex.transform(ss.createDataFrame(labels).toDF("id","label")).map { x => (x.getAs[String]("label"),x.getDouble(2).toInt) }.collect().toMap
    
    labelMaps.foreach(println)
    
    
    
    val prop= data.map(f=>(f._1,f._2,f._2.map(_._2).sum))
//    
    val wordsWithPros=prop.map(f=>{
      val numTerm=f._3
      val i= labelMaps.get(f._1).get
      
      
      val numberWithProp = f._2.map(x=>(x._1,x._2,x._2.toDouble/numTerm,idf(toHash.indexOf(x._1)),theta.apply(i, toHash.indexOf(x._1))))
      
      (f._1,numberWithProp.sortBy(_._2))
    })
    
    wordsWithPros.collect().foreach{
      f=>{
        val file=new File("/home/hadoop/result/words-with-vec/"+f._1)
        val prints=new PrintWriter(file)
        f._2.foreach(y=>{
        prints.println(y._1,y._2,y._3,y._4,y._5)  
        })
        
        prints.flush()
        prints.close()
        
      }
      
      
    }
    

  }
  
//  def word2Vec(ss:SparkSession):Unit={
//    
//    val word2Vec=new Word2Vec()
//                  .setInputCol("words").setOutputCol("result")
//                  .setVectorSize(3).setMinCount(0)
//    
//    val table = ss.table("splitTxt") 
//    val model = word2Vec.fit(table)
//    
//    
//    model.write.overwrite().save("file:///home/hadoop/result/word2Vec-tag")
//                  
//  }
  
}