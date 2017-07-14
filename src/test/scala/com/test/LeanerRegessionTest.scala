package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression

object LeanerRegessionTest {
  
  
  def main(args:Array[String]):Unit={
    
    val ss=SparkSession.builder().master("local[*]").appName("LRTest").getOrCreate()
    
    val sc=ss.sparkContext
    
    val data=ss.read.format("libsvm").load("file:///home/hadoop/ds/spark-2.1.1-bin-hadoop2.6/data/mllib/sample_libsvm_data.txt")
    data.show()
    val lr = new LogisticRegression()
             .setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = lr.fit(data)
    
  }
  
}