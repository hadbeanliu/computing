package com.recommendengine.compute.lib.quality

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import com.recommendengine.compute.lib.recommendation.ComputingTool

class Optimizer extends ComputingTool{
  
  
  
    def read ={
      
      null
    }
  
  
    def run()={
      
      null
    }
    
  def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put)){
    
  }
 
  
  
}