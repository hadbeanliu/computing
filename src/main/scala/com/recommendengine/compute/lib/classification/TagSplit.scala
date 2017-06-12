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
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover

class TagSplit extends ComputingTool{
  private val log = LoggerFactory.getLogger(classOf[WordSplit])
  private val STOP_WORD_PATH = "/computing/mining/data/stopWord.txt"

  private val DEFAULT_OUTPUT = "tmp_data_table"
  def read = {

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
//    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
    val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
    val tagCol = if (this.args.get("category.col") != null) this.args.get("category.col").asInstanceOf[String] else "f:t"
    val textCol = if (this.args.get("tag.col") != null) this.args.get("tag.col").asInstanceOf[String] else "kw"

    val default = if (source == null) input else source
    val scan = new Scan()
    scan.addColumn(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes)
    scan.addFamily(textCol.getBytes);
    println(source, default, ">>>>>>>>>>>>>>>", args)
    import collection.JavaConversions._
    val dataSource = HbaseServer.get(default, scan, sc, result => {

      
      val category = Bytes.toString(result.getValue(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes))
      val content =  result.getFamilyMap(textCol.getBytes)
      if(content==null||category==null)
        println(Bytes.toString(result.getRow))
      (category, content.keySet().map{Bytes.toString}.toArray)
    })
    dataSource
  }

  def run = {

    val result = new HashMap[String, Any]
    
    val data=read
    
    val minContentSize=if(this.args.get("minContentSize")==null) 20 else this.args.get("minContentSize").asInstanceOf[String].toInt
    val ss=SparkSession.builder().getOrCreate()
    
    import ss.implicits._
    
    val df=data.toDF("label","setence")
    
    val stopWord=ss.sparkContext.textFile(STOP_WORD_PATH).collect()
    
    
    val stopWordRemover=new StopWordsRemover
    stopWordRemover.setStopWords(stopWord)
    
    stopWordRemover.setInputCol("setence").setOutputCol("words")
    val df2=stopWordRemover.transform(df)
    println(df2.count())
   
    df2.createOrReplaceTempView("splitTxt")
//    df2.select("label", "words").createOrReplaceTempView("splitTxt")
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