package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil

class BaseUserTagItemSimilarRecommender extends ComputingTool {

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE).asInstanceOf[String].split(",")

    val table = bizCode + ":" + input(0)

    val scan = HbaseServer.buildScan(Array(("tags", null)), null, null, null)
    
//    val userWithTag=HbaseServer.find(table, family, q, raw)
    

  }

  def run= {
    val dataModel = read 
    null
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {

  }

}