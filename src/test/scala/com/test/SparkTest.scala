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

object SparkTest {

  def main(args: Array[String]): Unit = {
    
    val ss=SparkSession.builder().appName("wordmodel test").master("local").getOrCreate()
     
    
    
    
    val model =Word2VecModel.load("file:///home/hadoop/result/word2Vec-content")
//    
    val readbuff=new BufferedReader(new InputStreamReader(System.in))
    var tmp="";
    while(true){
      tmp =readbuff.readLine()
      
      model.findSynonyms(tmp, 15).show(15)
      
    }
    
    

  }
 
  def NaiveBayesTest() {}

}
case class Webpage(val title: String, val source: String, val content: String, val catagory: String, val tags: String)