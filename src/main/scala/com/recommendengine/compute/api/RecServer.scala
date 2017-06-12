package com.recommendengine.compute.api

import java.util.HashSet
import java.util.Set
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.restlet.Component
import org.restlet.data.Protocol
import org.restlet.ext.jaxrs.JaxRsApplication
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.collect.Queues
import com.recommendengine.compute.api.impl.ComputingServerPoolExecutor
import com.recommendengine.compute.api.impl.ConfManager
import com.recommendengine.compute.api.impl.MQManager
import com.recommendengine.compute.api.impl.RAMTaskManager
import com.recommendengine.compute.api.resource.TaskResource
import com.recommendengine.compute.api.resource.TasteResource

import javax.ws.rs.core.Application
import com.recommendengine.compute.api.resource.MiningResource
import com.recommendengine.compute.api.resource.DBResource
import org.apache.spark.sql.SparkSession

object RecServer {

  val port: Int = 9999

  val SERVER_NAME: String = "REC_SYS_TASK"

  val gobalConf=new SparkConf().setAppName(RecServer.SERVER_NAME).setMaster("local[*]")
  
//  val sconf = new SparkConf().setAppName(RecServer.SERVER_NAME)
  gobalConf.set("spark.kryoserializer.buffer.max.mb", "1024m")
  gobalConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //  sconf.set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=256M")
  
  def main(args: Array[String]) = {

    val server = new RecServer
    server.start
    //    println(RecServer.sconf.get("spark.kryoserializer.buffer.max.mb"))
    //    println("start ActiveMQ")
    //    Listener.start

  }
}

class RecServer(port: Int) extends Application with Server {

  @transient val log: Logger = LoggerFactory.getLogger("com.recommendengine.compute.api.RecServer")

  @transient var tmr: TaskManager = null

  @transient var mqmr: MQManager = null

  @transient var confmr: ConfManager = null

  @transient var component: Component = null
  def this() = this(9999)

  {
    val runnables: BlockingQueue[Runnable] = Queues.newArrayBlockingQueue(100);
    val executor: ComputingServerPoolExecutor = new ComputingServerPoolExecutor(1, 100, 1, TimeUnit.HOURS, runnables)

    tmr = new RAMTaskManager(executor, RecServer.gobalConf)
    confmr = new ConfManager
    mqmr = new MQManager(confmr)


    component = new Component();
    component.getLogger().setLevel(Level.parse("INFO"));
    component.getServers().add(Protocol.HTTP, RecServer.port);
    val childContext = component.getContext().createChildContext();

    val application = new JaxRsApplication(childContext);
    application.add(this);

    childContext.getAttributes().put(RecServer.SERVER_NAME, this);

    component.getDefaultHost().attach(application);

  }

  def beforStart = {}

  def start = {
    log.info("{ miningserver starting at port:" + RecServer.port + " }")
    try {
      component.start()

    } catch {
      case e: Exception => e.printStackTrace()
    }
    log.info("start success!")
  }

  override def getClasses(): Set[Class[_]] = {

    val classes: Set[Class[_]] = new HashSet[Class[_]]
    classes.add(classOf[TasteResource])
    classes.add(classOf[TaskResource])
    classes.add(classOf[MiningResource])
    classes.add(classOf[DBResource])
    classes

  }

}
