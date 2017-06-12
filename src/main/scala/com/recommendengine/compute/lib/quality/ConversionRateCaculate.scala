package com.recommendengine.compute.lib.quality

import com.recommendengine.compute.lib.recommendation.SparkJob
import com.recommendengine.compute.lib.recommendation.ComputingTool
import org.apache.hadoop.hbase.client.Result
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.filter.FilterList

class ConversionRateCaculate extends ComputingTool {

  private val UP_INDEX_CODE = "click"

  private val DOWN_INDEX_CODE = "view"

  def read= {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]

    val table = bizCode + ":" + input

    val filter = new PrefixFilter(ssCode.getBytes)
    val fList = new FilterList(filter)
    //    val filter=new SingleColumnValueFilter("behavior".getBytes)
    val scan = HbaseServer.buildScan(Array((null, null)), null, null, fList)

    HbaseServer.flatGet(table, scan, sc, null)
    
  }

  def run()= {
    
    
    null
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {

    
    
  }

}