package com.recommendengine.compute.api

import java.util.List

import com.recommendengine.compute.api.model.TaskConfig
import com.recommendengine.compute.api.impl.Task

trait TaskManager {
  
  
  def create(config:TaskConfig)
  
  def abort(code:String)
  
  def stop(code:String)
  
  def list(state:String):List[Task]
 
  def get(taskId:String):Task
  
  def start(taskId:String)
} 