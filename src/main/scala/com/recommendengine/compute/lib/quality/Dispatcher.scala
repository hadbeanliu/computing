package com.recommendengine.compute.lib.quality

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.ValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing


/**
 *  input read user recored from {#user_behavior_table} ,and find the recored which from the recommend pan
 *  
 *  output get the recored and sort by the timestamp and save to {#feedback} 
 *  
 *  default the schame   raw #timestamp, family #pv|uv  qualify #sscode/002type  value #cnt
 *  
 */
class Dispatcher extends ComputingTool {

  private val day: Int = 1

  private val sdf = new SimpleDateFormat("yyyy/MM/dd")

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]

    val table = bizCode + ":" + input

    val traceId = "traceId" + Computing.KEY_VALUE_SPLIT_SIGN + ssCode

    //    val filter=new SingleColumnValueFilter("behavior".getBytes)
    val qfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^[view|click|consume]{1}.*"))
    val vfilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(traceId))
    val filterList = new FilterList(Operator.MUST_PASS_ALL, qfilter, vfilter)
    val scan = HbaseServer.buildScan(Array(("behavior", null)), null, null, filterList)

    HbaseServer.flatGet(table, scan, sc,  result =>
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
              ((time,btype), (user, item, amt, cnt))
    
            }
    
          }
        )
    

  }

  def run() = {

        var data = read 

//    val data = sc.textFile("/input/report/recored.txt", 2).map { x =>
//      {
//        val field = x.split(Computing.KEY_KEY_SPLIT_SIGN)
//        val user = field(0)
//        val typeAndItem = field(1).split(Computing.KEY_VALUE_SPLIT_SIGN)
//        val item = typeAndItem(1)
//        val btype = typeAndItem(0)
//        val arr = field(2).split(Computing.VALUE_KEY_SPLIT_SIGN)
//
//        var amt: Double = 1
//        var cnt = 1
//        var time = ""
//        for (kv <- arr) {
//          if (kv.startsWith("amt"))
//            amt = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1).asInstanceOf[Double]
//          else if (kv.startsWith("cnt"))
//            cnt = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1).asInstanceOf[Int]
//          else if (kv.startsWith("time"))
//            time = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1)
//        }
//        ((time, btype), (user, item, amt, cnt))
//
//      }
//    }
//    println(data.count())
//
//    data.take(10).foreach(println)
    val PVPre = data.map(f => (f._1, f._2._4))

    val pvPerDay = PVPre.reduceByKey(_ + _)

    val pv = pvPerDay.map(f => (f._1._1, ("pv", f._1._2, f._2)))

    val uvPerDay = data.groupByKey().map(f => (f._1, f._2.size))

    val uv = uvPerDay.map(f => (f._1._1, ("uv", f._1._2, f._2)))

    //format to (time,(pv,type,cnt))
    val all = uv.union(pv).groupByKey()
    
    all.foreach(println)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    
    write[(String, Iterable[(String, String, Int)])](all, op => {

      val key = op._1
      val put = new Put(key.getBytes)

      val it = op._2.iterator

      while (it.hasNext) {
        val tag = it.next()
        put.add(tag._1.getBytes, (ssCode+Computing.KEY_VALUE_SPLIT_SIGN+tag._2).getBytes, Bytes.toBytes(tag._3))

      }

      (new ImmutableBytesWritable, put)
    })

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