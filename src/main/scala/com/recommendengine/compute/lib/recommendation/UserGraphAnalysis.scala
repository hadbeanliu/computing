package com.recommendengine.compute.lib.recommendation

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

/**
 * 该类用于计算用户画像，用户画像，跟据用户对物品的行为和物品的属性，系统对用户的画像进行分析
 *
 * 默认输入  input: user_item_score_list：【family=>behavior,q=>sscode,value=>itemID=value..】,item_meta_table:[family=>keyWords,q=>ssCode,value=> tag;tag...],
 * 默认输出  output:user_behavior_list:[family=>tags,q=>ssCode:sys,value=>tag=weight:tag=weight.....]
 */

class UserGraphAnalysis extends ComputingTool {

  private val DEFAULT_OUTPUT = "user_message_table"

  private val DEFAULT_INPUT_1 = "user_item_score_list"
  private val DEFAULT_INPUT_2 = "item_meta_table"
  
  val behavior_weight = 0.2
  //  val 

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

//    val input = this.args.get(Computing.INPUT_TABLE).asInstanceOf[String].split(",");
    val table1 = bizCode + ":" + DEFAULT_INPUT_1

    val scan1 = HbaseServer.buildScan(Array(("bhv", Array(ssCode))), null, null, null)
    val behavior = HbaseServer.flatGet(table1, scan1, sc, op => {

      val cell = op.rawCells()(0)
      val id = Bytes.toString(CellUtil.cloneRow(cell))
      val keys = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)
      for (key <- keys) yield {
        val kv = key.split(Computing.KEY_VALUE_SPLIT_SIGN)
        (id, kv(0), kv(1).toDouble)
      }
    })
    val DEFAULT_SPLIT_SIGN=","
    val table2 = bizCode + ":" + DEFAULT_INPUT_2

    val scan2 = HbaseServer.buildScan(Array(("pps", Array("kw"))), null, null, null)
    val items = HbaseServer.flatGet(table2, scan2, sc, p => {
      val cell = p.rawCells()(0)
      val id = Bytes.toString(CellUtil.cloneRow(cell))
      val keys = Bytes.toString(CellUtil.cloneValue(cell)).split(DEFAULT_SPLIT_SIGN)
      Array((id, keys))

    })

    (behavior, items)

  }

  def run = {

    val (behavior, items) = read
    //key by item id  (item,(userid,prefs))
    
    val itemWithUser = behavior.map(f=>(f._2,(f._1,f._3))).groupByKey()
    val broadCast=sc.broadcast(itemWithUser.collectAsMap())
    println(sc.getExecutorMemoryStatus," >>>1<<<<<")
    items.take(20).foreach(x=>{x._2.foreach { y => print(y,"--") };println})
    
    
    val usertag=items.filter(f=>broadCast.value.contains(f._1)).filter(f=>broadCast.value.contains(f._1)).flatMap{
      val v=broadCast.value
      itag=>{
        val userPrefs=v.get(itag._1).get
        userPrefs.flatMap(x=>itag._2.map { tag => ((x._1,tag),x._2) })
      }
    }
  

    val result = usertag.reduceByKey(_ + _)
    val maxX = result.map(_._2).max()
    val minX = result.map(_._2).min()
    val std = maxX - minX
    

    val finalResult = result.map(f => (f._1._1, f._1._2, (f._2 - minX) / std)).groupBy(_._1)
    
    finalResult.take(20).foreach(println)
    
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val q = ssCode + Computing.KEY_KEY_SPLIT_SIGN + "sys"
    broadCast.destroy()
    write[(String, Iterable[(String, String, Double)])](finalResult, op => {
      val put = new Put(op._1.getBytes)
      op._2.foreach {
        x =>
          put.add("g".getBytes, x._2.getBytes, Bytes.toBytes(x._3))

      }
      (new ImmutableBytesWritable, put)
    })

    null
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

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