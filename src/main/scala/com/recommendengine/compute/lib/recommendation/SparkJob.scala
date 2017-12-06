package com.recommendengine.compute.lib.recommendation

import org.apache.spark.SparkContext
import java.util.Map


trait SparkJob{
  
  def setArgs(args:java.util.Map[String,Any])
  
  def setSpark(sc:SparkContext)
  
  def getSpark:SparkContext
  
  def run():Map[String,Any]
  
}