package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD

trait ReadWritable{
  
  
   
  
  protected  var input:String=null
  
  protected  var output:String=null
  
  
  def setInput(input:String)=this.input=input
  
  def setOutput(output:String)=this.output=output
  
  
  def read [item:ClassTag] (scan:Scan,tableName:String,op:Result=>Array[item]):RDD[item]
  
 
  def write()
  
  
}