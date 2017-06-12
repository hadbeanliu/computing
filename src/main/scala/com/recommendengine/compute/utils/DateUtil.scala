package com.recommendengine.compute.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  val PATTARN_DATE_FORMAT = "yyyy/MM/dd"

  val sdf = new SimpleDateFormat(PATTARN_DATE_FORMAT)

  val MILLION_OF_DAY = 24 * 60 * 60* 1000
  
  val MILLION_OF_WEEK=this.MILLION_OF_DAY*7

  val DAY_OF_MOUTH = 1

  val MONTH_OF_DATE = 2

  val YEAR_OF_DATE = 3
  
  val WEEK_OF_YEAR=4

  def dateCompare(date1: Date, date2: Date, field: Int): Int = {

    val day1 = parser(date1, field)
    val day2 = parser(date2, field)

    Math.abs(day1 - day2)

  }

  def dateCompare(date1: String, date2: String, field: Int): Int = {

    val day1 = sdf.parse(date1)
    val day2 = sdf.parse(date2)

    dateCompare(day1, day2, field)

  }
  
  def interval(start:Long,end:Long):Int={
    ((end-start)/MILLION_OF_WEEK).toInt
    
  }
  
  
  def interval(start:String,end:String,unit:Int=WEEK_OF_YEAR):Int={
    
    interval(sdf.parse(start).getTime,sdf.parse(end).getTime)
    

  }

  private def parser(date: Date, field: Int): Int = {

    (date.getTime / MILLION_OF_DAY).asInstanceOf[Int]

  }

}