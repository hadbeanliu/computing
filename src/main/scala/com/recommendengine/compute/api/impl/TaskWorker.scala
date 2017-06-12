package com.recommendengine.compute.api.impl

import java.text.MessageFormat
import java.util.HashMap

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.recommendengine.compute.api.model.Algorithm
import com.recommendengine.compute.api.model.State
import com.recommendengine.compute.api.model.TaskConfig
import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.StringUtils

import sun.reflect.misc.ReflectUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class TaskWorker(val taskConf: TaskConfig, conf: Configuration, sconf: SparkConf) extends Runnable {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[TaskWorker])
  private var info: TaskInfo = new TaskInfo(State.IDEL, getGenerateId, taskConf, null, new java.util.HashMap[String, Any]())
  private var result = new HashMap[String, Any]()

  val ss:SparkSession =SparkSession.builder().getOrCreate()
  
  val sc=ss.sparkContext

  override def run() {
    result.clear()
    val algs = taskConf.algorithmFlow
    
    while (!algs.isEmpty()) {
      val alg = algs.poll()
      val tool = createTools(alg, conf)

      if (alg.args != null) {
        println(alg.args)
        val paramMap: java.util.List[java.util.Map[String, String]] = new Gson().fromJson(alg.args, new TypeToken[java.util.List[java.util.Map[String, String]]]() {}.getType)

        val args = new HashMap[String, Any]()

        val size = paramMap.size()
        for (i <- 0 until size) {
          val param = paramMap.get(i)
          args.put(param.get("code"), param.get("val"))
          println(param.get("code"), param.get("val"))
        }
        println(alg.input, alg.output, alg.mainClass)
        args.put(Computing.INPUT_TABLE, alg.input)
        args.put(Computing.OUTPUT_TABLE, alg.output)
        tool.setArgs(args)

        try {
          LOG.info(s"start the task $tool, with the info : $info")
          println(s"start the task $tool, with the info : $info")
          
          val key=alg.mainClass.substring(alg.mainClass.lastIndexOf(".")+1)
          tool.setSpark(sc)
          result.put(key, tool.run())
          
          LOG.info(s"end the task $tool")
          println(s"end the task $tool")
          
        } catch {
          case e: Exception => {
            LOG.error("can not run this task", e.printStackTrace())
            getInfo.state = State.FAILED
            e.printStackTrace()
          }

        }

      }

    }
    val rowKey=taskConf.ssCode
    val put=new Put(rowKey.getBytes)
    put.addImmutable("log".getBytes, Bytes.toBytes(System.currentTimeMillis()), new Gson().toJson(result).getBytes)
    HbaseServer.save(put, conf, taskConf.bizCode+":history")
//    println(new Gson().toJson(result))

  }

  private def createTools(alg: Algorithm, conf: Configuration): ComputingTool = {

    if (StringUtils.isEmpty(alg.mainClass))
      throw new IllegalArgumentException("Algorithm's main class can not be null")

    val clazz = Class.forName(alg.mainClass)

    val tool = ReflectUtil.newInstance(clazz).asInstanceOf[ComputingTool]

    if (tool.isInstanceOf[Configurable])
      tool.setConf(conf)

    tool

  }

  private def getGenerateId: String = {

    MessageFormat.format("{1}-{2}-{3}-{4}", taskConf.bizCode, taskConf.ssCode, taskConf.configId, System.currentTimeMillis() + "")
  }

  def getInfo = info

}

case class TaskInfo(var state: State.State, var taskId: String, var config: TaskConfig, var args: Map[String, String], var result: java.util.Map[String, Any])