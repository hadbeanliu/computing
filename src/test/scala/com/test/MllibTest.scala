package com.test

import org.apache.spark.mllib.clustering.KMeans
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream
import scala.util.Random
import java.util.Date
import java.util.Calendar

object MllibTest {

  def main(args: Array[String]) {

//    val file = new File("/home/hadoop/Documents/examples")
//    if (!file.exists())
//      file.mkdir()
//    val sys = Calendar.getInstance
//    sys.set(2014, 1, 20)
//    val begin = sys.getTime.getTime
//    val now = System.currentTimeMillis()
//
//    val span =( now - begin).toInt
//
//    val write = new PrintWriter(new FileOutputStream(new File("/home/hadoop/Documents/examples/userPrefs2.txt"),true))
//
//    for (i <- 0 until 100) {
//      val nums = Random.nextInt(5)
//      for (j <- 0 until nums) {
//        val prefs = new StringBuffer();
//
//        prefs.append(i).append("::").append(Random.nextInt(100)).append("::").append(Random.nextInt(5)+1).append("::").append(begin+Random.nextInt(span)).append("\n")
//        write.write(prefs.toString())
//        //            
//      }
//    }
//    write.flush()
//    write.close()
  }
}
          
