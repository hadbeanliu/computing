package com.recommendengine.compute.api

import java.util.List

import com.recommendengine.compute.api.impl.TaskInfo
import com.recommendengine.compute.api.model.TaskConfig

trait TaskManager {
  
  
  def create(config:TaskConfig)
  
  def abort(code:String)
  
  def stop(code:String)
  
  def list(state:String):List[TaskInfo]
 
  def get(taskId:String):TaskInfo
  
  def start(taskId:String)
} 