package com.recommendengine.compute.utils

object StringUtils {

  def isEmpty(str: String)=if (str == null || str.length() == 0) true else false

  //translate a Int to a New Int,x is target,mod is the max feature of this
  def getNonNegatived(x: Int, mod: Int): Int = {

    val raw = x % mod

    raw + (if (raw < 0) mod else 0)

  }

  def main(args: Array[String]): Unit = {

    println(12312)
  }

}