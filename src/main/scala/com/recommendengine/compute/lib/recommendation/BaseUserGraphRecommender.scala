package com.recommendengine.compute.lib.recommendation

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import com.recommendengine.compute.metadata.Computing

class BaseUserGraphRecommender extends ComputingTool {
  
  private val DEFAULT_INPUT_1="user_item_score_list"
  private val DEFAULT_INPUT_2="item_meta_table"
  
  private val DEFAULT_SPLIT_SIGN=","
  
  def read:Any={

    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE).asInstanceOf[String].split(",");
    val table1 = bizCode + ":" + input(0)
    
    
    val scan1 = HbaseServer.buildScan(Array(("bhv", Array(ssCode))), null, null, null)
    val behavior = HbaseServer.get(table1, scan1, sc, op => {
    
      val cell = op.rawCells()(0)
      val id = Bytes.toString(CellUtil.cloneRow(cell))
      val keys = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)
      val itemAndPref=for (key <- keys) yield {
        val kv = key.split(Computing.KEY_VALUE_SPLIT_SIGN)
        (kv(0), kv(1).asInstanceOf[Double])
      }
      (id,itemAndPref)
    })

    val table2 = bizCode + ":" + input(1)

    val scan2 = HbaseServer.buildScan(Array(("pps", Array("kw"))), null, null, null)
    val items = HbaseServer.get(table2, scan2, sc, p => {
      val cell = p.rawCells()(0)
      val id = Bytes.toString(CellUtil.cloneRow(cell))
      val keys = Bytes.toString(CellUtil.cloneValue(cell)).split(DEFAULT_SPLIT_SIGN)
      Array((id, keys))

    })

    (behavior, items)

  }
  
  def run={
    
    val (upref,itag)=read
    
     
     
    
    null
    
  }
  
  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {
    
  }
}