package com.recommendengine.compute.api.impl

import java.util.HashMap

import org.apache.hadoop.conf.Configuration

import com.recommendengine.compute.api.MQListener
import com.recommendengine.compute.metadata.Computing

class MQManager(confs: ConfManager) {

  private val listeners = new HashMap[String, MQListener]()

  def createListener(conf: Configuration): String = {

    val force = conf.getBoolean("force", false)

    try {
      if (conf == null)
        throw new NullPointerException("Configuration can not be null")

      val listenerId = conf.get(Computing.COMPUTING_ID)

      if (listeners.get(listenerId) != null) {
        
        return listenerId;

      }

      val listener = new MQListener(conf)
      listeners.put(listenerId, listener)
      listener.start()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return "faild"
      }
    }

    "success"
  }

}