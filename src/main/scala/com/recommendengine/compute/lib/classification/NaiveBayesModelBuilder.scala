package com.recommendengine.compute.lib.classification

import scala.reflect.ClassTag

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import java.util.HashMap
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.spark.sql.SparkSession

/**
 * 保存分词结果到hbase，分词规则如下:key:ssCode;family:result;q:split:category;value：()
 */

// call split
class NaiveBayesModelBuilder extends ComputingTool {

  var modelType: String = "multinomial"

  var lambda: Double = 1.0

  def read = {

    
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
    
   
    
    val scan = new Scan()
    scan.addFamily("result".getBytes)

    val filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(ssCode+"_tfidf"))
    scan.setFilter(filter)

   
    val dataSource = HbaseServer.get(input, scan, sc, result => {

      val cell = result.rawCells()(0)

          LabeledPoint.parse(Bytes.toString(CellUtil.cloneValue(cell)))

    })
//    val ss=SparkSession.builder().master("local").getOrCreate()
//    val datas=
    dataSource
  }

  def run = {

    val result=new HashMap[String,Any]
    if (this.args.get("modelType") != null) modelType = this.args.get("modelType").asInstanceOf[String]

    if (this.args.get("lambda") != null) lambda = this.args.get("lambda").asInstanceOf[String].toDouble

//    result.put("args", Array(("modelType",modelType),("lambda",lambda)))
    result.put("args", args)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val input = read
    
    val hashing = new HashingTF
    

    val model = NaiveBayes.train(input, lambda, modelType)
    
    val predict=input.map { x => (x.label,model.predict(x.features)) }
    val count=predict.count
    val cnt=1.0 * predict.filter(f=>(f._1==f._2)).count
    
    result.put("accuracy", cnt/count)
    println(s"$cnt,$count,accuracy=${cnt/count}")
    val path = "/computing/mining/model/" + bizCode + "-" + ssCode

    val p = new Path(path)

    val fs = FileSystem.get(getConf)

    if (fs.exists(p)) {
      fs.delete(p, true)
    }
    println(s"the model save path is $path")
    model.save(getSpark, path)
//    println(Runtime.getRuntime.freeMemory(),13)
    result
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

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