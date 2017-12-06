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

class TaskWorker(val taskConf: TaskConfig, conf: Configuration) extends Runnable {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[TaskWorker])
  private var result = new HashMap[String, Any]()
  private val info = new Task(status = "IDEL", taskId = "123123", result = new HashMap[String, Any], args = new HashMap[String, String])

//  val ss: SparkSession = SparkSession.builder().appName(taskConf.bizCode).master("local[*]").getOrCreate()

//  val sc = ss.sparkContext

  override def run() {
    result.clear()
    val algs = taskConf.algorithmFlow

    while (!algs.isEmpty()) {
      val alg = algs.poll()
      val tool = createTools(alg, conf)
      if (alg.args != null) {

        val args = new HashMap[String, Any]()

        args.putAll(alg.args)
        tool.setArgs(args)

        try {
          LOG.info(s"start the task $tool, with the info : $info")

          val key = alg.clazz.substring(alg.clazz.lastIndexOf(".") + 1)
//          tool.setSpark(sc)
//          result.put(key, tool.run())

          LOG.info(s"end the task $tool")

        } catch {
          case e: Exception => {
            LOG.error("can not run this task", e.printStackTrace())
            getInfo.status = "FAILD"
            e.printStackTrace()
          }

        }

      }

    }
    val rowKey = taskConf.ssCode
//    val put = new Put(rowKey.getBytes)
//    put.addImmutable("log".getBytes, Bytes.toBytes(System.currentTimeMillis()), new Gson().toJson(result).getBytes)
//    HbaseServer.save(put, conf, taskConf.bizCode + ":history")
    //    println(new Gson().toJson(result))

  }

  private def createTools(alg: Algorithm, conf: Configuration): ComputingTool = {

    if (StringUtils.isEmpty(alg.clazz))
      throw new IllegalArgumentException("Algorithm's main class can not be null")

    val clazz = Class.forName(alg.clazz)

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
case class Task(var intput: String = "default", var output: String = "", var numCheckPoint: Int = 3, var status: String, var taskId: String, result: HashMap[String, Any], args: HashMap[String, String])
