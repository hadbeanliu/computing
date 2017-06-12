package com.recommendengine.compute.api

import java.util.ArrayList

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.qpid.amqp_1_0.jms.Destination
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.recommendengine.compute.db.hbase.FileMappingToHbase
import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.metadata.Computing

import javax.jms.JMSException
import javax.jms.Message
import javax.jms.MessageConsumer
import javax.jms.MessageListener
import javax.jms.Session
import javax.jms.TextMessage

class MQListener(val conf: Configuration) {

  val log: Logger = LoggerFactory.getLogger(classOf[MQListener])
  private var startUp:Boolean=false

  def start(): Boolean = {

    if (conf == null)
      throw new NullPointerException("configuration can not be null")

    val user = conf.get(Computing.MESSAGE_ACTIVITY_USER)
    val password = conf.get(Computing.MESSAGE_ACTIVITY_PASSWORD)
    val host = conf.get(Computing.MESSAGE_ACTIVITY_HOST)
    val port = conf.getInt(Computing.MESSAGE_ACTIVITY_PORT, 5673)
    val destination = conf.get(Computing.MESSAGE_ACTIVITY_DIST)

    require(user != null & password != null & host != null & destination != null)

    log.info("start activemq,user=" + user + ",host=" + host + ":" + port)

    val connFactory = new ConnectionFactoryImpl(host, port, user, password)
    val dest: Destination = if (destination.startsWith("topic://")) {
      new TopicImpl(destination);
    } else {
      new QueueImpl(destination);
    }

    val connection = connFactory.createConnection()
    connection.setClientID("topClient1");
    connection.start();

    val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val consumer: MessageConsumer = session.createDurableSubscriber(new TopicImpl(destination), this.getClass.getName)
    log.info("Starting mq success and waiting for message")

    startUp=true
    beforeStart
    consumer.setMessageListener(new MessageListener() {
      def onMessage(msg: Message): Unit = {

        if (msg.isInstanceOf[TextMessage]) {
          val txtMsg = msg.asInstanceOf[TextMessage]

          try {
            val txt = txtMsg.getText

            if ("SHUTDOWN".equals(txt)) {

            } else {

              println(txt)
              SaveMessageHbase.save(txt)

            }

          } catch {
            case e: JMSException => {
              e.printStackTrace()

            }
          }

        } else {
          println("UNEXCEPTED MESSAGE TYPE:" + msg.getClass)
        }

      }
    })

    false

  }
  
  def isStart=this.startUp
  
  
  def beforeStart={
    
      val bizCode=conf.get(Computing.COMPUTING_ID)
      require(bizCode!=null&&bizCode.trim()!="")
      val tablesMap=FileMappingToHbase.readMappingFile(null)
      val descs=tablesMap.values().iterator()
      while(descs.hasNext()){
        
         val desc=descs.next()
         try{
         FileMappingToHbase.createTable(desc, conf, bizCode)
         log.info("create table :", bizCode+new String(desc.getName))
         }catch{
           case e:Exception=>{
             e.printStackTrace()
             log.error("faild to create table ["+bizCode+new String(desc.getName)+"]>>"+e.getMessage, e.getCause)
             }
           
           
         }
      }
    
  }

}


private object SaveMessageHbase {

  def save(msg: String): Boolean = {

    val strs = msg.split(Computing.KEY_KEY_SPLIT_SIGN)

    if (strs.length != 5)
      throw new IllegalArgumentException("UNEXCEPTED MESSAGE SCHEME:" + msg)

    val put = new Put(strs(1).getBytes)
    put.add(strs(2).getBytes, strs(3).getBytes, strs(4).getBytes)
    HbaseServer.save(put, null, strs(0))

    true
  }

  def saveList(msgs: java.util.List[String]): Boolean = {
    val it = msgs.iterator()
    val puts: java.util.List[Put] = new ArrayList
    var tableName=""
    while (it.hasNext()) {

      
      val msg = it.next()
      val strs = msg.split(Computing.KEY_KEY_SPLIT_SIGN)

      if (strs.length != 5)
        throw new IllegalArgumentException("UNEXCEPTED MESSAGE SCHEME:" + msg)
      tableName=strs(0)
      val put = new Put(strs(1).getBytes)
      put.add(strs(2).getBytes, strs(3).getBytes, strs(4).getBytes)
      puts.add(put)
    }

    
    HbaseServer.saveList(puts, null, tableName)

    true
  }

  def main(args: Array[String]): Unit = {

    val users = 10000
    val item = 200

    val beh = Array("consume", "view", "collect")

    val maxRating = 5
    val perMaxRecored = 10

    for (i <- 1 to users) {
      val list=new ArrayList[String]
      for (j <- 0 to Random.nextInt(perMaxRecored)) {
        val buff = new StringBuffer
        buff.append("emall:user_behavior_table").append(Computing.KEY_KEY_SPLIT_SIGN)
          .append(i).append(Computing.KEY_KEY_SPLIT_SIGN)
          .append("behavior").append(Computing.KEY_KEY_SPLIT_SIGN)
          .append(beh(Random.nextInt(3))).append(Computing.KEY_VALUE_SPLIT_SIGN).append(Random.nextInt(item)).append(Computing.KEY_KEY_SPLIT_SIGN)
          .append("cmt").append(Computing.KEY_VALUE_SPLIT_SIGN).append(Random.nextFloat()).append(Computing.VALUE_KEY_SPLIT_SIGN)
          .append("time").append(Computing.KEY_VALUE_SPLIT_SIGN).append("2016/7/10")

        println(buff.toString())
        list.add(buff.toString())
      }
      this.saveList(list)
    }

  }

}
