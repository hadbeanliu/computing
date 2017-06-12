package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.spark.ml.feature.HashingTF
import org.apache.hadoop.hbase.client.Scan
import java.util.HashMap

class BaseContentCBRecommender extends ComputingTool {

  def read()= {    
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE)

    val table = bizCode + ":" + input

    val scan = HbaseServer.buildScan(Array(("behavior", Array(ssCode))), null, null, null)
    
    HbaseServer.flatGet(table, scan, sc, op =>
      {
        for (cell <- op.rawCells()) yield {
              val key=Bytes.toString(CellUtil.cloneRow(cell))
              val properties=Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)
              for(kv<-properties)yield{
                val field=kv.split(Computing.KEY_VALUE_SPLIT_SIGN)
                (key,(field(0),field(1)))
              }
        }
      })

  }

  def run = {
    
    val result=new HashMap[String,Any]
    val hashTF=new HashingTF
    
//    val feature = read 
    result
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {

  }

}