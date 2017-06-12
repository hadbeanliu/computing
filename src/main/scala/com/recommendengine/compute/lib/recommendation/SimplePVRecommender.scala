package com.recommendengine.compute.lib.recommendation

import java.text.SimpleDateFormat
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import java.util.HashMap
import com.recommendengine.compute.utils.DateUtil
import java.util.Date
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import java.util.ArrayList
import com.recommendengine.compute.exception.UnImplementMethodException

class SimplePVRecommender extends ComputingTool {

  private val RATAIN_DAYS = "ratain_days"

  private val SUFFIX = ".weight"

  private var ratainDays = -1

  private val sdf = new SimpleDateFormat("yyyy/MM/dd")

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val input = this.args.get(Computing.INPUT_TABLE)

    if (this.args != null && this.args.get(RATAIN_DAYS) != null)
      ratainDays = args.get(RATAIN_DAYS).asInstanceOf[Int]

    val table = bizCode + ":" + input

    val scan = HbaseServer.buildScan(Array(("behavior", null)), null, null, null)
    var data = HbaseServer.flatGet(table, scan, sc,  result =>
      {
        val user = Bytes.toString(result.getRow)
        for (cell <- result.rawCells()) yield {
          val typeAndItem = Bytes.toString(CellUtil.cloneQualifier(cell)).split(Computing.KEY_VALUE_SPLIT_SIGN)
          val item = typeAndItem(1)
          val btype = typeAndItem(0)
          val arr = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)

          var amt: Double = 1
          var cnt = 1
          var time = ""
          for (kv <- arr) {
            if (kv.startsWith("amt"))
              amt = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1).asInstanceOf[Double]
            else if (kv.startsWith("cnt"))
              cnt = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1).asInstanceOf[Int]
            else if (kv.startsWith("time"))
              time = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1)
          }
          ((user, item), (btype, amt, cnt, time))

        }

      }
    )
    data

  }

  def run()= {

    var data = read 

    val types = data.map(_._2._1).distinct().collect()
    val ssCode=getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode=getConf.get(Computing.COMPUTING_ID)
    val output=this.args.get(Computing.OUTPUT_TABLE).asInstanceOf[String]

    val topK = this.args.get("topK").asInstanceOf[String].toInt

    val weights = new HashMap[String, Double]()
    for (t <- types) {

      if (this.args.containsKey(t + SUFFIX)) {

        weights.put(t, this.args.get(t + SUFFIX).asInstanceOf[String].toDouble)
      }
    }

    if (ratainDays <= 0) {
      val dateLine = sdf.format(new Date().getTime - ratainDays * DateUtil.MILLION_OF_DAY)
      data = data.filter(f => f._2._4.compareTo(dateLine) >= 0)
    }

    data = data.filter(f => weights.containsKey(f._2._1))

    val ItemWithSize = data.map(f => (f._1._2, f._2._3)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(topK)

    val put = new Put("simple_pv_rec".getBytes)
    val buff = new StringBuffer
    ItemWithSize.foreach(f => {
      buff.append(f._1).append(Computing.KEY_VALUE_SPLIT_SIGN).append(f._2).append(Computing.VALUE_KEY_SPLIT_SIGN)
    })
    put.add("result".getBytes, ssCode.getBytes, buff.toString().getBytes)
    HbaseServer.save(put, getConf, bizCode+":"+output)
    null
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {
    
    throw new UnImplementMethodException(getClass.getName+".write()")
  }

}