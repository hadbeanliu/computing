package com.recommendengine.compute.lib.classification

import java.util.HashMap

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.lib.recommendation.ComputingTool
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.NaiveBayesModel
import scala.collection.mutable.Seq
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

class ClassifyModelBuilder extends ComputingTool {
  
  
  val DEFAULT_NAME="naivebaye"
  
  val FEATURE_TABLE="features"
  
  val LABLE_NAME="labelIndex"
  
  val FEATURE_ROW:String = "features"
  
  
   def read:Any = { }


   def run ={
     
    
    val result=new HashMap[String,Any]()
    
    val ss=SparkSession.builder().getOrCreate()
    
    
    import ss.implicits._
    val features=ss.table(FEATURE_TABLE)
    
   
    val naivebaye=new NaiveBayes()
    naivebaye.setLabelCol(LABLE_NAME).setFeaturesCol(FEATURE_ROW)
    
    val model = naivebaye.fit(features)
    val predict=model.transform(features)
    
    val acculation=new MulticlassClassificationEvaluator()
                   .setLabelCol(LABLE_NAME)
                   .setPredictionCol("prediction")
                   .setMetricName("accuracy")
                   

    val accu=acculation.evaluate(predict)
    
    println("...................................准确率:>>>"+accu);
    
    val modelPath=getConf.get("default.model.path")+"/"+ getConf.get(Computing.COMPUTING_ID)+"_"+getConf.get(Computing.COMPUTING_BITCH_ID)
    model.write.overwrite().save(modelPath+"/"+NaiveBayesModel.getClass.getSimpleName)
   
//    val model =new NaiveBayesModel
    
    result
  }
  def write[item:ClassTag](obj:RDD[item],op:item=>(ImmutableBytesWritable,Put))={
    
    
  }
}