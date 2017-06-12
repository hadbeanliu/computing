package com.recommendengine.compute.lib.quality

import com.recommendengine.compute.lib.recommendation.SparkJob
import com.recommendengine.compute.lib.recommendation.ComputingTool
import org.apache.hadoop.hbase.client.Result
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put


class ResultEvaluate extends ComputingTool{
  
  
  def read:Any={
    
    
  }
  
  
  def run={
    null
    
  }
  
  def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put)){}
}