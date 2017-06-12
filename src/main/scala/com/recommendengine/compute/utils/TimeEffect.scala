package com.recommendengine.compute.utils

import java.text.SimpleDateFormat

class TimeEffect(var m:Double,val sys:Long) {
  
  def caculate(cur:Long,lastTime:Long):Double={
    if(m==null)
      m=0
      m*(cur-sys)/(lastTime-sys)+(1-m)   
  }
  
}


object TimeEffect{
  
  def main(args:Array[String])={
    println(Math.pow(Math.E, -Math.log(4)))
    println(decay(27,0)(2))
  }
  
  /**
   * 描述了随时间而衰败的函数
   * @param t:时间间隔，period:衰变周期，prefix:衰败开始时间，默认为0
   * 默认时间单位为周
   **/
  def decay(period:Int=27,prefix:Int=0)(t:Int):Double={
    
      if(t<=prefix)
        return 1
       Math.pow(Math.E, -Math.pow((t-prefix)*1.0/period, 2)*Math.log(2))
    
  }
  
  
}