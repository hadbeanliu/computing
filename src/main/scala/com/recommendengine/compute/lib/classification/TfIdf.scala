package com.recommendengine.compute.lib.classification

import java.util.HashMap

import scala.reflect.ClassTag

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{udf }
/**
 *
 *
 */
class TfIdf extends ComputingTool {

  def read = {
    
    
  }

  def run = {

    val result = new HashMap[String, Any]

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val minDocFreq = this.args.get("minDocFreq").asInstanceOf[String].toInt

    result.put("args", args)
    result.put("ss_code", ssCode)
    result.put("biz_code", bizCode)

    
    val ss=SparkSession.builder().getOrCreate()
    
    val data =ss.table("splitTxt")
    
//    val t = udf{term:Seq[_] => }
    

    result
  }

  private def store(put: Put) {
    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + output
    

    
    
  }

  private def writeToHdfs(path: String, str: String, overwrite: Boolean = false): Boolean = {

    val seedDir = new Path(path)
    val fs = FileSystem.get(getConf)

    try {
      if (!overwrite) {
        fs.deleteOnExit(seedDir)
      }
      val os = fs.create(seedDir)
      os.write(str.getBytes)
      os.flush()
      os.close()
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {

      fs.close()

    }

    true
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

    val output = this.args.get(Computing.OUTPUT_TABLE).asInstanceOf[String]
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + output

    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }
}