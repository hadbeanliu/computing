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
import org.apache.spark.ml.regression.LinearRegression

object SparkTest {

  def main(args: Array[String]): Unit = {
    
     val ss= SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    val data=ss.sparkContext.textFile("file:///home/hadoop/train/wordFreq.txt",1)
    data.map(x=> {
      val data = x.split(",")
      (if(data(0).substring(1).toInt/50==0) 0 else 1, data(1).substring(0, data(1).length - 1).toInt)
    }
    ).reduceByKey((x,y)=>x + y).sortBy(_._1).saveAsTextFile("file:///home/hadoop/train/result2.txt")
  }
 
  def NaiveBayesTest() {}

}
case class Webpage(val title: String, val source: String, val content: String, val catagory: String, val tags: String)