package com.recommendengine.compute.api.model

import com.google.gson.Gson

class Items {
  
  val itemList:Array[Item] =null 
  
  def toJson():String={
    val gson=new Gson
    gson.toJson(itemList)
    
  }
  
  
}


case class Item(id:String,value:Float,tsmp:Long)