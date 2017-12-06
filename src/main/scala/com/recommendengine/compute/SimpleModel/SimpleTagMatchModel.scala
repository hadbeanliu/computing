package com.recommendengine.compute.SimpleModel

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import org.apache.hadoop.hbase.client.Scan

object SimpleTagMatchModel {
  
}

class SimpleTagMatchModel extends ComputingTool{

  private val day: Int = 1

  private val sdf = new SimpleDateFormat("yyyy/MM/dd")

  def read = {
    
    val bizCode=getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    
    val lastDay=getConf.get("fetch.last.day", "7").toInt
    
    val scan =new Scan
    
    
  }

  def run() = {
    
    
    
    null
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {

    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + output

    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }
}

