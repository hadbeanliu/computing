package com.recommendengine.compute.api.impl

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.recommendengine.compute.api.TaskManager
import com.recommendengine.compute.api.model.Algorithm
import com.recommendengine.compute.api.model.TaskConfig
import com.recommendengine.compute.exception.UnImplementMethodException
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.StringUtils

import sun.reflect.misc.ReflectUtil
import java.util.HashMap
import org.apache.hadoop.hbase.HTableDescriptor
import com.recommendengine.compute.db.hbase.FileMappingToHbase
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class RAMTaskManager(val executor: ComputingServerPoolExecutor, val sconf: SparkConf) extends TaskManager {

  private val cmr: ConfManager = new ConfManager()

  override def create(task: TaskConfig) {

    val bizCode = task.bizCode
    val ssCode = task.ssCode

    val conf = if (StringUtils.isEmpty(task.configId)) {
      cmr.createConf(task)
      cmr.getConf(task.configId)
    } else cmr.getConf(task.configId)

    conf.set(Computing.COMPUTING_ID, bizCode)
    conf.set(Computing.COMPUTING_BITCH_ID, ssCode)
    conf.set(Computing.COMPUTING_CONF_ID, task.configId)
    //      conf.set(Computing.INPUT_TABLE, alg.input)
    //      conf.set(Computing.OUTPUT_TABLE ,alg.output)
    //      create table and namespace for this task 
    beforeCreateTask(conf)

    //      val tool = createTools(alg, conf)

    //      if (alg.args != null){
    //        println(alg.args)
    //        val paramMap:java.util.List[java.util.Map[String,String]]=new Gson().fromJson(alg.args, new TypeToken[java.util.List[java.util.Map[String,String]]](){}.getType)
    //        
    //        val args=new HashMap[String,Any]()
    //        
    //        val size=paramMap.size()
    //        for(i<-0 until size){
    //            val param=paramMap.get(i)
    //            args.put(param.get("code"), param.get("val"))
    //            println(param.get("code"), param.get("val"))
    //        }
    //        println(alg.input,alg.output,alg.mainClass)
    //        args.put(Computing.INPUT_TABLE, alg.input)
    //        args.put(Computing.OUTPUT_TABLE, alg.output)
    //        tool.setArgs(args)
    //        
    //      }
    
   
    val worker = new TaskWorker(task, conf, sconf)

    this.executor.execute(worker)
    this.executor.purge()

  }

  private def beforeCreateTask(conf: Configuration) {

    val bizCode = conf.get(Computing.COMPUTING_ID);
    if (bizCode == null)
      throw new IllegalArgumentException(" 业务代码不能为空");
    val tableName = conf.get("preferred.table.name");

    val tables: java.util.Map[String, HTableDescriptor] = FileMappingToHbase
      .readMappingFile(conf.get("hbase.default.mapping.file"))

    val values = tables.values().iterator()

    while (values.hasNext()) {

      if (tableName == null)

        FileMappingToHbase.createTable(values.next(), conf, bizCode);
    }

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

  override def abort(code: String) = {

  }

  override def stop(code: String) = {

  }

  override def list(state: String): java.util.List[TaskInfo] = {
    if (state == null || state == "ALL")
      executor.getAllTasks()
    else if (state == "RUNNING" || state == "IDLE")
      executor.getRunningTasks()
    else executor.getHistoryTasks();

  }

  override def get(taskId: String): TaskInfo = {
    val infos = executor.getAllTasks().iterator()
    while (infos.hasNext()) {
      val info = infos.next()
      if (info.taskId == taskId)
        return info
    }
    null
  }

  override def start(taskId: String) = throw new UnImplementMethodException("can't start the task:" + taskId + "; because umimplement")

}

