package com.recommendengine.compute.lib.recommendation

import java.util.Map

import org.apache.hadoop.conf.Configured
import org.apache.spark.SparkContext

abstract class ComputingTool extends Configured with SparkJob with Readable with Writable{
  
 
  protected var args:Map[String,Any]=null
  protected var sc:SparkContext=null
  
  def setSpark(sc:SparkContext)={
    this.sc=sc
  }
  
   def getSpark=this.sc
   
         
   def throwNotFoundException=throw new RuntimeException("Not Found Data Source")
  
   def setArgs(args:Map[String,Any])=this.args=args
   
   def timeAttenuate(time:Long,now:Long):Double= 1/(Math.log(time))
   
}