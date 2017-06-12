package com.recommendengine.compute.utils

import scala.collection.immutable.Map
import scala.util.parsing.json.JSON

object JsonParser {

  def parserFull(json: String, root: String): Map[String, Any] = {

    val option = JSON.parseFull(json)

    option match {

      case Some(m: Map[String, Any]) => if (root == null) m else m.get(root).get.asInstanceOf[Map[String, Any]]
      
      case None                      => Map[String, Any]()
      
      case other                     => throw new UnKnownException()
    }

  }

}

class UnKnownException(val message: String) extends Exception {

  def this() = this(null)
}