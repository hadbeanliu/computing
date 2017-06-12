package com.recommendengine.compute.api.impl

import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration

import com.recommendengine.compute.api.model.TaskConfig
import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.utils.StringUtils

class ConfManager {
  
    private val configurations=new HashMap[String,Configuration]()
    private val CONFIGURATION_DEFAULT="default"
  
    private val newConfigId:AtomicInteger=new AtomicInteger
    
    def getConf(confId:String):Configuration={
      
      if(confId==null)
        return configurations.get(CONFIGURATION_DEFAULT)
      
      configurations.get(confId)
      
    }
    
    
    def createConf(task:TaskConfig):String={
      
        val bizCode=task.bizCode
        val ssCode=task.ssCode
        
        if(StringUtils.isEmpty(bizCode)||StringUtils.isEmpty(bizCode))
                throw new  IllegalArgumentException("bizCode or ssCode can not be null")

        task.configId=String.valueOf(newConfigId.incrementAndGet())
        createConfiguration(task)
        
    }
    
    private def createConfiguration(task:TaskConfig):String={
      
       val conf=ComputingConfiguration.createWithHbase
        configurations.put(task.configId, conf)
       task.configId
    }
    
    private def getGeneratorId(bizCode:String,ssCode:String):String={
      
       bizCode+"-"+ssCode
      
    }
    
  
}