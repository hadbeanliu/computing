package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

object KMeans {
  
  def main(args:Array[String]):Unit={
    
    
    val ss=SparkSession.builder().appName("k-means").master("local[*]").getOrCreate()
    val sc=ss.sparkContext
    
    val data1= sc.parallelize(Seq("a","b","c"), 1)
    
    val data2 = sc.parallelize(Seq(1,2,3), 1)
    
    val data3=data1.zip(data2)
    
    data3.collect().foreach(println)
//    val kmeans = new KMeans().fit(dataset)

    
    
  }
  
  
}