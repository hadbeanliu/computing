package com.recommendengine.compute.lib.recommendation

import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.mapred.JobConf
import com.recommendengine.compute.metadata.Computing
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StopWordsRemover
import java.util.HashMap
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import com.recommendengine.compute.lib.classification.WordSplit
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Scan
import org.slf4j.LoggerFactory
import com.recommendengine.compute.utils.TextSplit
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.Cell
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.ml.feature.StringIndexer
import scala.io.Source
import java.io.PrintWriter
import java.io.OutputStream
import java.io.FileOutputStream
import com.google.gson.Gson
import com.recommendengine.compute.conf.ComputingConfiguration
import org.apache.spark.SparkConf

object MetaMatchModel {

  def main(args: Array[String]): Unit = {    
    
    val length=args.length
    var i=0;
    val arg=new java.util.HashMap[String,Any]
    while(i<length){
      
      if(args(i).startsWith("-")){
        arg.put(args(i).substring(1), args(i+1))
        i=i+2
      }else i = i +1
      
    }
    
    
    val sconf=new SparkConf().setAppName("test")
    
    val ss=SparkSession.builder().appName("simple").master("local[*]").config(sconf).getOrCreate()
    
    val sc=ss.sparkContext
    
    val conf=ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
//    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-test")

//    conf.set("default.model.path", "file:///home/hadoop/result/model")
    
    val tool=new MetaMatchModel
    tool.setSpark(sc)
    tool.setConf(conf)
    tool.setArgs(arg)
    
    
    arg.put(Computing.DATA_SOURCE, "headlines:item_meta_table")
    tool.setArgs(arg)
    
    tool.run
    

    

    
  }
}

class MetaMatchModel extends ComputingTool {
  private val log = LoggerFactory.getLogger(classOf[WordSplit])
  private val STOP_WORD_PATH = "/computing/mining/data/stopWord.txt"

  private val DEFAULT_OUTPUT = "tmp_data_table"
  def read = {

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    //    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
    val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]

    val default = if (source == null) input else source

    //    scan.setStartRow("982937380995139".getBytes)
    val day_to_release = if (this.args.get("Max_Day_Delay") == null) 3 else this.args.get("Max_Day_Delay").asInstanceOf[Int]
    val format = new SimpleDateFormat("YYYYMMddHH")
    def idReserve = (id: String) => { (3000000000000000L - id.toLong).toString }
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_YEAR, -1 * day_to_release)
    val stopRow = format.format(calendar.getTime()) + "000000";
    
//    val first = new Scan
//    first.setBatch(1)

    //    scan.setStartRow()
    val scan = new Scan()
    scan.addFamily("kw".getBytes)
    
//    scan.addColumn(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes);
    println(source, default, ">>>>>>>>>>>>>>>", args)
    val dataSource = HbaseServer.get(default, scan, sc, result => {
      
      result.listCells().toArray().map { x => Bytes.toString(CellUtil.cloneQualifier(x.asInstanceOf[Cell])) }
      
    })
    dataSource
  }

  def run = {

    val v=SparseVector
    val result = new HashMap[String, Any]
    val ss=SparkSession.builder().appName("metaToKey").getOrCreate()
    val data = read
    
    val tagWithLong=data.flatMap { x => x.map { y => (y,1) } }.reduceByKey(_ + _)
    var i=0
    val tags=tagWithLong.sortBy(_._2).map(_._1).collect()
    val map=new HashMap[String,Int]
    tags.foreach { x => {
      map.put(x, i)
      i +=1
    } }
    println(map.size())
    val pw=new PrintWriter(new FileOutputStream("/home/hadoop/result/tags.json"));
    pw.write(new Gson().toJson(map))
    pw.flush()
    pw.close()
    // val data = ss.sparkContext.wholeTextFiles("file:///home/hadoop/result/train").map(f=>(f._1.substring(f._1.lastIndexOf("/")+2),f._2))
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