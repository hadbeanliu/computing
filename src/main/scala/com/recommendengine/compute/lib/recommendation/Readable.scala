package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Scan

trait Readable {
  
    def read:Any 
}