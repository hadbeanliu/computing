package com.recommendengine.compute.api.resource

import java.io.File
import java.net.URLDecoder
import java.util.Map

import org.apache.hadoop.conf.Configuration

import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import com.recommendengine.compute.api.impl.TaskInfo
import com.recommendengine.compute.api.model.TaskConfig

import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam

object TaskResource {
  
  val jarDir="hdfs:///computing/jar/"

}

@Path(value = "/itask")
class TaskResource extends AbstractResource {
  
  
  @GET
  @Path(value="/upload/{jarName}")
  def uploadJar(@PathParam("jarName") jarName: String):Unit={
    
//    val file=new file
    val jar=new File(TaskResource.jarDir+jarName)
    
    if(!jar.exists())
       throw new NullPointerException("can not find jar package :"+jarName)
    
    
    
  }

  @GET
  @Path(value = "/abort/{taskId}/{force}")
  def abort(@PathParam("taskId") taskId: String, @PathParam("force") force: Boolean): Unit = {
    println(1231231231)

  }
  
  @POST
  @Path(value="/open-mq")
  def openMQ(config:String):String={
    
    val tmp=URLDecoder.decode(config, "UTF-8").substring(7)
   
    
    val args:Map[String,String]=new Gson().fromJson(tmp, new TypeToken[Map[String,String]](){}.getType)
    
    if(args==null||args.isEmpty())
      throwBadRequestException("bad request! not enough params")
      
/*    val host=args.get(Computing.MESSAGE_ACTIVITY_HOST).asInstanceOf[String]
    val port=args.get(Computing.MESSAGE_ACTIVITY_PORT).asInstanceOf[Int]
    val bizCode=args.get(Computing.COMPUTING_ID).asInstanceOf[String]
    val user=args.get(Computing.MESSAGE_ACTIVITY_USER).asInstanceOf[String]
    val pwd=args.get(Computing.MESSAGE_ACTIVITY_PASSWORD).asInstanceOf[String]
    val destiney= args.get(Computing.MESSAGE_ACTIVITY_DIST).asInstanceOf[String]*/
    
    val it=args.entrySet().iterator()
    val conf:Configuration=new Configuration
    while(it.hasNext()){
      val entry=it.next()
        conf.set(entry.getKey , entry.getValue)
    
    }
    this.mqmr.createListener(conf)
          
  }

  @POST
  @Path(value = "/create/{tsmp}") //   @Consumes(value=Array(MediaType.TEXT_HTML))
  def create(taskConf: String): String = {
    println(taskConf)
    val encode=URLDecoder.decode(taskConf.substring(9),"UTF-8")

    val task: TaskConfig = new Gson().fromJson(URLDecoder.decode(taskConf.substring(9),"GBK"), classOf[TaskConfig])
    if (task == null)
      throwBadRequestException("task config can't be null")
    if (task.algorithmFlow == null || task.algorithmFlow.size() == 0)
      throw new IllegalArgumentException("algrothm flow is null")
    try {
      val begin=Runtime.getRuntime.freeMemory()
     this.tmr.create(task)
     println(Runtime.getRuntime.freeMemory()-begin)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return e.getMessage
        
      }

    }
    null

  }
  
  @GET
  @Path(value="/list")
  def list(@PathParam("state" )state:String):java.util.List[TaskInfo]={
    
    this.tmr.list(state)
    
  }
  
  @GET
  @Path(value="/get")
  def get(@PathParam("taskId") taskId:String):TaskInfo={
    this.tmr.get(taskId)
  }

}