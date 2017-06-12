package com.recommendengine.compute.utils

object TableUtil {
  
  
  def hostReserve(host:String,user:String):String={
    
    var _prefix=user.charAt(0).toString()+user.charAt(user.length()-1).toString().toUpperCase()
  
    val arr=host.split("\\.")
    
    
    _prefix+"_"+arr.apply(arr.length/2)
    
  }
  def getNonNagatived(x: Int, mod: Int): Int = {

    val raw = x % mod

    raw + (if (raw < 0) mod else 0)

  }
  
  
  def main(args:Array[String]){
    
    println(hostReserve("work.net", "admin"))
    
  }
  
}