package com.recommendengine.compute.lib.classification

import java.util

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.TextSplit
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

class SimHashTrain extends ComputingTool{
  override def run(): util.Map[String, Any] = {


    val data=read
    val dataWithFreq = data.filter(cnt=>cnt!=null&&cnt.length>10).map(TextSplit.process(_))
      .map(arr=>{
        val map =new mutable.HashMap[String,Int]()
        arr.map((_,1)).foreach(x=>map.put(x._1,map.getOrElse(x._1,0)+1))
        map.toArray
      })


    null
  }

  override def write[item: ClassTag](data: RDD[item], op: (item) => (ImmutableBytesWritable, Put)): Unit = ???

  override def read = {

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    //    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
    val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
    val textCol = if (this.args.get("content.col") != null) this.args.get("content.col").asInstanceOf[String] else "p:t"

    val default = if (source == null) input else source
    val scan = new Scan()
    scan.addColumn(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes);
    val dataSource = HbaseServer.get(default, scan, sc, result => {

      val row = Bytes.toString(result.getRow)
      val content = Bytes.toString(result.getValue(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes))
      content

    })
    dataSource

  }
}
