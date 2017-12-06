package com.recommendengine.compute.computing

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LinearSVC
object SVMClassify {

  def main(args:Array[String]):Unit = {

    
    val spark = SparkSession.builder().appName("linearSVM").master("local[*]").getOrCreate()
    val training = spark.read.format("libsvm").load("file:///home/hadoop/ds/spark-2.2/data/mllib/sample_libsvm_data.txt")

    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    // Fit the model
    val lsvcModel = lsvc.fit(training)

    // Print the coefficients and intercept for linear svc
    println(s" FeatureLength: ${lsvcModel.coefficients.size} Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
  
    
  }

}
