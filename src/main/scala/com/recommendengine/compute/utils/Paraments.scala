package com.recommendengine.compute.utils

import java.io.BufferedInputStream
import java.io.FileInputStream
import java.util.Properties

class Paraments(val fileName:String) {
  
  val prop:Properties=new Properties
  
  def getValue(key:String):String={
    
    try{
      
      val in=new BufferedInputStream(new FileInputStream(fileName))
      prop.load(in)
      prop.getProperty(key)
      
    }catch{
      
      case e:Exception=>{
        e.printStackTrace()
        " not find"
        }
      
    }
    
  }
  
  
}