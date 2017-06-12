package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put

class PearsonRank extends ComputingTool{
  
  def read:Any={
    
    
  } 
  
  def run={
    
    null
  }
  
  
  def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put))={
    null
  }
}