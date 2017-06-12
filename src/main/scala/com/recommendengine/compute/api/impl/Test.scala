package com.recommendengine.compute.api.impl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test {

  val port: Int = 9999

  val SERVER_NAME: String = "REC_SYS_TASK"


  def main(args: Array[String]) = {


    val sconf=new SparkConf().setAppName("test").setMaster("local[*]")
  
    val sc:SparkContext=new SparkContext(sconf)
    
    //
//    val task=new TaskConfig()
//    println(task.ss_code,task.code)
//    val work=new TaskWorker(task,RecConfiguration.createWithHbase,sc)
//    work.run()

  }
}