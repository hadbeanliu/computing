package com.test

object HttpTest {
  
  
  def main(args0:Array[String]):Unit={
    
    val str="asdf123sdfdfdddd"
    
    val strs=str.format("\\d")
    println(strs)
  }
  
}

case class People(name:String,age:Int)