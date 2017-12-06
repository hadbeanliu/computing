package com.recommendengine.compute.computing

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

object LogistRegession {
  
  def main(args:Array[String]):Unit={
    
    val ss=SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    
    val dataModel = ss.read.format("libsvm").load("file:///home/hadoop/ds/spark-2.2/data/mllib/sample_libsvm_data.txt")
    
    val lr = new LogisticRegression().setRegParam(0.3).setMaxIter(11).setElasticNetParam(0.8)
    
    val lrModel = lr.fit(dataModel)
    println(s"Feature coeffience: ${lrModel.coefficients},intercept: ${lrModel.intercept}")
    
  }
  
}