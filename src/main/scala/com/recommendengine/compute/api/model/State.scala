package com.recommendengine.compute.api.model

object State  extends Enumeration{
  
  type State=Value
      
  val IDEL,STOP,RUNNING,KILLED,FAILED,FINISHED,SUCCESS=Value
  
}