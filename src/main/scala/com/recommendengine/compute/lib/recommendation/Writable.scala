package com.recommendengine.compute.lib.recommendation

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

trait Writable {
  
  
  def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put))
  
  
}