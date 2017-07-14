package com.recommendengine.compute.lib.classification

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.slf4j.LoggerFactory

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.TextSplit
import java.util.HashMap
import com.recommendengine.compute.utils.GeneratorIdBuild
import scala.util.Random
import com.chenlb.mmseg4j.example.Complex

/**
 * 保存分词结果到hbase，分词规则如下:key:ssCode;family:result;q:split:category;value：()
 */

// call split
class WordSplit extends ComputingTool {
  private val log = LoggerFactory.getLogger(classOf[WordSplit])
  private val STOP_WORD_PATH = "/computing/mining/data/stopWord.txt"

  private val DEFAULT_OUTPUT = "tmp_data_table"
  def read = {

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
    val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
    val tagCol = if (this.args.get("category.col") != null) this.args.get("category.col").asInstanceOf[String] else "f:t"
    val textCol = if (this.args.get("content.col") != null) this.args.get("content.col").asInstanceOf[String] else "p:t"

    val default = if (source == null) input else source
    val scan = new Scan()
    scan.addColumn(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes)
    scan.addColumn(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes);
    println(source, default, ">>>>>>>>>>>>>>>", args)
    val dataSource = HbaseServer.get(default, scan, sc, result => {

      
      val category = Bytes.toString(result.getValue(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes))
      val content = Bytes.toString(result.getValue(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes))
      if(content==null||category==null)
        println(Bytes.toString(result.getRow))
      (category, content)
    })
    dataSource
  }

  def run = {

    val result = new HashMap[String, Any]

    import scala.collection.JavaConverters._

    log.info(s"start task $this")
    //    val split=this.args.get("split.tool").asInstanceOf[String]

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    result.put(Computing.COMPUTING_TASK_ID, GeneratorIdBuild.build(bizCode, ssCode, this.getClass.getSimpleName))
    result.put(Computing.COMPUTING_ID, bizCode)
    result.put(Computing.COMPUTING_BITCH_ID, ssCode)
    result.put("task_status", "health")

    log.info("Init word spilt tool: JIEBA-ANALYSIS")
    val data = read
    result.put("number_data", data.count())
    val begin = System.currentTimeMillis()
    val stopWord = sc.textFile(STOP_WORD_PATH, 1).collect()
    val boradCast = sc.broadcast(stopWord)
    
    val dataLength=data.count()
    println(data.count(), data.filter(f => f._1 != null && f._2 != null).count())
    //    val splitText = data.map(f => (f._1, TextSplit.process(f._2.trim().replaceAll("(\r|\n)", "")).asScala.toSeq.filterNot { x => boradCast.value.contains(x) }.mkString(",")))
//    val splitText = data.mapPartitions(x => {
//      val stop = boradCast.value
//      x.map(f => (f._1, TextSplit.process(f._2.trim().replaceAll("(\r|\n)", "")).asScala.toSeq.filterNot { x => boradCast.value.contains(x) }.mkString(",")))
//
//    }).cache()
    
    
    val splitText = data.mapPartitions(x => {
      val stop = boradCast.value
      x.map(f => (f._1, TextSplit.process(f._2)))

    })
    splitText.take(2).foreach(println)
    
    val num = getConf.getInt("sample.data.number", 10)
    result.put(Computing.SAMPLE_DATA_SCAN, splitText.take(num))

    val length = (Math.log(0x800) / Math.log(dataLength) + 3).toInt
    println("长度:=", length, System.currentTimeMillis() - begin)
//    write[(String, String)](splitText, op => {
//
//      val id = Random.nextString(length) + "_" + ssCode + "_" + "split"
//      val put = new Put(id.getBytes)
//      val family = "rs".getBytes
//      val q = op._1.getBytes
//
//      val v = op._2.getBytes
//      put.add(family, q, v)
//      (new ImmutableBytesWritable, put)
//    })
//    boradCast.unpersist()
    result
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val tableName = bizCode + ":" + DEFAULT_OUTPUT

    println(tableName, output)
    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }
}