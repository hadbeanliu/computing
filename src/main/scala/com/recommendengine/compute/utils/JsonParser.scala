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
  
  def main(args:Array[String]):Unit={
    val str = s"{name:jack}"
    println(parserFull(str,"name"))
    val map =Map[String,Any]("a"->2,("b",2))
    println(map)
  }

}

class UnKnownException(val message: String) extends Exception {

  def this() = this(null)
}