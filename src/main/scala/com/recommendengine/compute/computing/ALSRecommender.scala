package com.recommendengine.compute.computing

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.spark.ml.recommendation.ALS.Rating

object ALSRecommender {
  
  
  def main(args:Array[String]): Unit = {
    
     val userNum=100
     val itemNum=200
     
     val actNum=10
     
     val acts=for(i<-0 to 100)yield{
       val n=Random.nextInt(actNum+1)
       for(j<- 0 until n)yield
            Rating(i,Random.nextInt(itemNum),Random.nextFloat())
       
     }
     
     
    
    val ss=SparkSession.builder().appName("als").master("local[*]").getOrCreate()
//     ss.createDataFrame(acts)
//    val data=ss.read.text(path)
    
    val als=new ALS().setAlpha(0.1).setImplicitPrefs(false).setMaxIter(7).setRank(7).setUserCol("")
    
    
  }
}