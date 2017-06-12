package com.recommendengine.compute.exception

class UnImplementMethodException(message:String) extends RuntimeException(message){
  
  val _exception=new RuntimeException(message)
  
  
  
}