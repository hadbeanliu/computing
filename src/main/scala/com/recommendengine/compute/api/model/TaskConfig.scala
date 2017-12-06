package com.recommendengine.compute.api.model

import java.io.Serializable
import java.util.Map
import java.util.Queue

case class TaskConfig(var configId:String,bizCode:String,ssCode:String,algorithmFlow:Queue[Algorithm])

case class TaskJob(args:Map[String,Object],bizCode:String,ssCode:String,tsmp:Long)

case class Algorithm(defautArgs:java.util.Map[String,String],args:java.util.Map[String,String],clazz:String,jar:String,algCode:String)

