package com.recommendengine.compute.api.resource

import java.io.File
import java.net.URLDecoder

import com.google.gson.Gson
import com.recommendengine.compute.api.impl.Task
import com.recommendengine.compute.api.model.TaskConfig

import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam

object TaskResource {

  val jarDir = "hdfs:///computing/jar/"

}

@Path(value = "/itask")
class TaskResource extends AbstractResource {

  @GET
  @Path(value = "/upload/{jarName}")
  def uploadJar(@PathParam("jarName") jarName: String): Unit = {

    //    val file=new file
    val jar = new File(TaskResource.jarDir + jarName)

    if (!jar.exists())
      throw new NullPointerException("can not find jar package :" + jarName)

  }

  @GET
  @Path(value = "/abort/{taskId}/{force}")
  def abort(@PathParam("taskId") taskId: String, @PathParam("force") force: Boolean): Unit = {
    println(1231231231)

  }

  @GET
  @Path("/daily/train/{tName}/{tId}")
  def dailyTask(@PathParam("tName")tName:String,@PathParam("tId")tId:String,param:Map[String,String]): Unit ={

    require(tName!=null,"task Name is null")
    require(tId!=null,"task id is null")
    require(param!=null&&param.size != 0,"task id is null")
  }


  @POST
  @Path(value = "/start")
  def start(job: String, @PathParam("biz_code") biz_code: String, @PathParam("ss_code") ss_code: String, @PathParam("tsmp") tsmp: String): String = {

    val tasks: TaskConfig = new Gson().fromJson(job, classOf[TaskConfig])

    if (tasks == null)
      throwBadRequestException("task config can't be null")
    if (tasks.algorithmFlow == null || tasks.algorithmFlow.size() == 0)
      throw new IllegalArgumentException("algrothm flow is null")
    try {
      val begin = Runtime.getRuntime.freeMemory()
      this.tmr.create(tasks)
      println(Runtime.getRuntime.freeMemory() - begin)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return e.getMessage

      }

    }

    null
  }

  @POST
  @Path(value = "/create") //   @Consumes(value=Array(MediaType.TEXT_HTML))
  def create(taskConf: String, @PathParam("tsmp") tsmp: String): String = {
    println(taskConf)
    val encode = URLDecoder.decode(taskConf.substring(9), "UTF-8")

    val task: TaskConfig = new Gson().fromJson(URLDecoder.decode(taskConf.substring(9), "GBK"), classOf[TaskConfig])
    if (task == null)
      throwBadRequestException("task config can't be null")
    if (task.algorithmFlow == null || task.algorithmFlow.size() == 0)
      throw new IllegalArgumentException("algrothm flow is null")
    try {
      val begin = Runtime.getRuntime.freeMemory()
      this.tmr.create(task)
      println(Runtime.getRuntime.freeMemory() - begin)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return e.getMessage

      }

    }
    null

  }

  @GET
  @Path(value = "/list")
  def list(@PathParam("state") state: String): java.util.List[Task] = {

    this.tmr.list(state)

  }

  @GET
  @Path(value = "/get")
  def get(@PathParam("taskId") taskId: String): Task = {
    this.tmr.get(taskId)
  }

}

case class ComputingJob(biz_code: String, ss_code: String, jobs: Array[MetaJob])
case class MetaJob(algCode: String, args: java.util.Map[String, String], clazz: String, jar: String)
