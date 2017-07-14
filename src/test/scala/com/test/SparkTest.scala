package com.test

import org.apache.spark.sql.SparkSession

import com.recommendengine.compute.lib.quality.Dispatcher
import com.recommendengine.compute.lib.recommendation.BaseItemCFRecommender
import com.recommendengine.compute.lib.recommendation.BaseRatingRuleRecommend
import com.recommendengine.compute.lib.recommendation.BaseUserCFRecommender
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.lib.recommendation.UserGraphAnalysis
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
import scala.io.Source
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import com.recommendengine.compute.conf.ComputingConfiguration
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.IDFModel

object SparkTest {

  def main(args: Array[String]): Unit = {
//    
//   
    
    val ss=SparkSession.builder().appName("wordmodel test").master("local").getOrCreate()
//     
    val conf=ComputingConfiguration.create()
    val bsCode="headlines_user-analys"
     val model=IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName)
     
     println(model.idf.size)
  }
 
  def NaiveBayesTest() {}

}
case class Webpage(val title: String, val source: String, val content: String, val catagory: String, val tags: String)