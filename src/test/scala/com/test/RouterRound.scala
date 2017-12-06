package com.test

object RouterRound {
  var cnt = 0
  var count = 0
  def main(args: Array[String]): Unit = {

    val step = 12

    val arr = Array.fill(25, 25)(0)
    arr(11)(11) = 1
    arr(11)(12) = 1
    next(9, arr, 11, 12)
    println(4 * cnt)
    println(count)
  }

  def next(currStep: Int, arr: Array[Array[Int]], i: Int, j: Int): Boolean = {
    if (currStep == 0)
      return true

    if (arr(i)(j + 1) == 0) {

      arr(i)(j + 1) = 1
      if (next(currStep - 1, arr, i, j + 1)) {
        cnt += 1
        arr(i)(j + 1) = 0
      }
    }
    if (arr(i)(j - 1) == 0) {
      count+=1
      arr(i)(j - 1) = 1
      if (next(currStep - 1, arr, i, j - 1)) {
        cnt += 1
        arr(i)(j - 1) = 0
      }
    }
    if (arr(i + 1)(j) == 0) {
      count+=1
      arr(i + 1)(j) = 1
      if (next(currStep - 1, arr, i + 1, j)) {
        cnt += 1
        arr(i + 1)(j) = 0
      }
    }
    if (arr(i - 1)(j) == 0) {
      count+=1
      arr(i - 1)(j) = 1
      if (next(currStep - 1, arr, i - 1, j)) {
        cnt += 1
        arr(i - 1)(j) = 0
      }
    }

    false
  }

}