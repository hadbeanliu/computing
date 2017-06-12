package com.recommendengine.compute.lib.recommendation

import java.text.SimpleDateFormat
import java.util.Date
import java.util.HashMap

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.DateUtil
import com.recommendengine.compute.utils.TimeEffect
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.BinaryComparator

class BaseRatingRuleRecommend extends ComputingTool with Readable with Writable {

  private val RATAIN_DAYS = "ratain_days"

  private val SUFFIX = ".weight"

  private var ratainDays = -1

  private val DEFAULT_INPUT = "user_behavior_table"
  
  private val DEFAULT_OUTPUT="user_item_score_list"

  private val sdf = new SimpleDateFormat("yyyy/MM/dd")

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val input = getConf.get(Computing.INPUT_TABLE)

    if (this.args.get(RATAIN_DAYS) != null)
      ratainDays = args.get(RATAIN_DAYS).asInstanceOf[Int]

    val deadLine = if (ratainDays > 0) {
      System.currentTimeMillis() - 24l * 60 * 60 * 1000 * ratainDays
    } else 0l

    val table = bizCode + ":" + DEFAULT_INPUT

    val scan = HbaseServer.buildScan(Array(("bhv", Array("type", "cnt", "rating", "time"))), null, null, null)
    scan.setCaching(50)
    if (deadLine > 0) {
      val filter = new SingleColumnValueFilter("bhv".getBytes, "time".getBytes, CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(deadLine)))
      scan.setFilter(filter)
    }

    HbaseServer.get(table, scan, sc, result =>
      {
        val id = Bytes.toString(result.getRow).split("_")
        val (user, item) = (id(0), id(2))
        val act = Bytes.toString(result.getValue("bhv".getBytes, "type".getBytes))
        val time = Bytes.toLong(result.getValue("bhv".getBytes, "time".getBytes))
        val cnt = Bytes.toInt(result.getValue("bhv".getBytes, "cnt".getBytes))

        val rating = Bytes.toFloat(result.getValue("bhv".getBytes, "rating".getBytes))
        ((user, item), (act, rating, cnt, time))

      })

  }

  def run() = {

    val result = new HashMap[String, Any]
    val now = System.currentTimeMillis()

    var data = read

    val prefix = if (this.args.get("time.prefix") == null) 0 else this.args.get("time.prefix").asInstanceOf[String].toInt

    val period = if (this.args.get("time.decay.period") == null) 27 else this.args.get("time.decay.period").asInstanceOf[String].toInt

    val decay = TimeEffect.decay(period = period, prefix = prefix)_

    val types = data.map(_._2._1).distinct().collect()

    val weights = new HashMap[String, Double]()
    for (t <- types) {

      if (this.args.containsKey(t + SUFFIX)) {

        weights.put(t, this.args.get(t + SUFFIX).asInstanceOf[String].toDouble)
      }
    }


    data = data.map(f => (f._1, (f._2._1, f._2._2 * decay(DateUtil.interval(f._2._4, now)).toFloat, f._2._3, f._2._4)))

    data = data.filter(f => weights.containsKey(f._2._1))

    val ratings = data.map { case (k, v) => (k, weights.get(v._1).asInstanceOf[Double] * v._2) }

    val userItemWithScore = ratings.reduceByKey(_ + _).map(f => (f._1._1, f._1._2, f._2)).groupBy(_._1)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    // write to hbase  tableName=  bizCode:output
    write[(String, Iterable[(String, String, Double)])](userItemWithScore, f => {
      val put = new Put(f._1.getBytes)
      val buff = new StringBuffer
      for ((u, i, v) <- f._2) {
        buff.append(i).append(Computing.KEY_VALUE_SPLIT_SIGN).append(v).append(Computing.VALUE_KEY_SPLIT_SIGN)

      }
      put.add("bhv".getBytes, ssCode.getBytes, buff.toString().getBytes)
      (new ImmutableBytesWritable(), put)

    })
    result
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) {

    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + DEFAULT_OUTPUT

    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }

}