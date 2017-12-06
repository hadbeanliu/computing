package com.recommendengine.compute.lib.classification

import java.util

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DataExport extends ComputingTool{
  override def write[item: ClassTag](data: RDD[item], op: (item) => (ImmutableBytesWritable, Put)): Unit = ???

  override def run(): util.Map[String, Any] = {

    LOG.info("开始导入数据...")
    LOG.info("开始读取数据...")
    val data=read
    LOG.info("成功读取数据..."+data.count())
    data.filter(_.length>25).saveAsTextFile("/computing/data")
    LOG.info("成功导出数据...")
    null
  }

  override def read = {

      val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
      val bizCode = getConf.get(Computing.COMPUTING_ID)
      //    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
      val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
      val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
      val tagCol = if (this.args.get("category.col") != null) this.args.get("category.col").asInstanceOf[String] else "f:t"
      val textCol = if (this.args.get("content.col") != null) this.args.get("content.col").asInstanceOf[String] else "p:t"

      val default = if (source == null) input else source
      val scan = new Scan()
      scan.addColumn(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes)
      scan.addColumn(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes);
      val dataSource = HbaseServer.get(default, scan, sc, result => {


        val category = Bytes.toString(result.getValue(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes))
        val content = Bytes.toString(result.getValue(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes))
        if(content==null||category==null)
          ""
        else category+"/0003"+content

      })
      dataSource

  }
}
