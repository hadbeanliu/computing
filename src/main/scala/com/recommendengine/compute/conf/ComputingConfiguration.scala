package com.recommendengine.compute.conf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object ComputingConfiguration extends Configuration{
  
  
  def create():Configuration={
    
      val conf=new Configuration
      conf.addResource("computing-site.xml")
      conf
  }
  
  def createWithHbase:Configuration={
    
      val conf=HBaseConfiguration.create()
      conf.addResource("computing-site.xml")
      conf
    
  }
  
}