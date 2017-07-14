package com.recommendengine.compute.utils

import java.text.DecimalFormat

class NumberFormat {
  
  private val  df = new DecimalFormat("#.00");
  
  
  def format(number:Double):String={
    df.format(number)
  }
}