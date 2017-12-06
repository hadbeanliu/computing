package com.recommendengine.compute.lib.classification

import java.util.HashMap

import com.google.gson.Gson

import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import com.recommendengine.compute.lib.recommendation.ComputingTool
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.ml.classification.NaiveBayesModel

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
    
    LOG.info("开始任务 :"+this.getClass)
    val naivebaye=new NaiveBayes()
    naivebaye.setLabelCol(LABLE_NAME).setFeaturesCol(FEATURE_ROW)

     LOG.info("开始训练模型 :"+NaiveBayesModel.getClass.getName)

     val model = naivebaye.fit(features)

     LOG.info("成功训练模型 :"+NaiveBayesModel.getClass.getName)
     val modelPath=getConf.get("default.model.path")+"/"+ getConf.get(Computing.COMPUTING_ID)+"_"+getConf.get(Computing.COMPUTING_BITCH_ID)
     LOG.info("保存模型 :"+modelPath+"/"+NaiveBayesModel.getClass.getSimpleName)

     model.write.overwrite().save(modelPath+"/"+NaiveBayesModel.getClass.getSimpleName)
     LOG.info("保存模型成功 ....")
     LOG.info("计算准确率 ....")

     val predict=model.transform(features.randomSplit(Array(0.8,0.2))(1))

    val acculation=new MulticlassClassificationEvaluator()
                   .setLabelCol(LABLE_NAME)
                   .setPredictionCol("prediction")
                   .setMetricName("accuracy")


    val accu=acculation.evaluate(predict)
     LOG.info("本次训练 准确率 :"+accu)

    println("...................................准确率:>>>"+accu);


   LOG.info("成功完成本次全部流程，所有结果参数："+new Gson().toJson(result))
//    val model =new NaiveBayesModel
    
    result
  }
  def write[item:ClassTag](obj:RDD[item],op:item=>(ImmutableBytesWritable,Put))={
    
    
  }
}