package com.recommendengine.compute.lib.classification

import java.util.HashMap

import com.google.gson.Gson

import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.ml.feature.IDF
import org.apache.zookeeper.proto.GetChildren2Request
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.linalg.DenseVector


class FeatureSelect extends ComputingTool{
  
  
  val DEFAULT_NAME="TFIDF"
  
  val INPUT_TABLE="words"
  
  val OUTPUT_TABLE="features"

  def read = {

//    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
//    val bizCode = getConf.get(Computing.COMPUTING_ID)
//
//    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
//
//    println(s"ssCode=$ssCode,bizCode=$bizCode,input=$input")
//
//    val scan = new Scan()
//    scan.addFamily("result".getBytes)
//
//    val filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(ssCode + "_split"))
//    scan.setFilter(filter)
//    val dataSource = HbaseServer.get(input, scan, sc, result => {
//
//      val cell = result.rawCells()(0)
//      val t = Bytes.toString(CellUtil.cloneQualifier(cell))
//      val v = Bytes.toString(CellUtil.cloneValue(cell)).split(",")
//      (t, v)
//    })
//
//    dataSource

  }

  def run = {
    
    val minDocFreq=if(this.args.get("minDocFreq")==null) 5 else this.args.get("minDocFreq").asInstanceOf[String].toInt
    LOG.info("开始任务："+ new Gson().toJson(this.args))
    val result = new HashMap[String, Any]
    
    val ss=SparkSession.builder().getOrCreate()
    LOG.info("开始转换为TF")

    val hashingTf=new HashingTF
    hashingTf.setInputCol("words").setOutputCol("rowFeatures")
    
    val words=ss.table("splitTxt")
    
    val toHash=new org.apache.spark.mllib.feature.HashingTF()

    val maps=words.select("words").rdd.flatMap { x => x.getAs[Seq[String]]("words").map { y => (y,toHash.indexOf(y)) } }.distinct().collect().toMap
    val tf=hashingTf.transform(words)
//    val transform =udf {
//        vec:Vector=>
//          val sparseV=vec.toSparse
//          val size=sparseV.indices.size
//          val values=sparseV.values.map { x => x/size }
//          new SparseVector(sparseV.size,sparseV.indices,values)
//      
//    }
//    val tf=tf1.withColumn("rowFeatures", transform(tf1("rowFeatures")))
        
    val idf=new IDF().setInputCol("rowFeatures").setOutputCol("features").setMinDocFreq(minDocFreq)
    LOG.info("开始训练 IDFModel")
    val idfModel=idf.fit(tf)

    LOG.info("开始转换为 TFIDF")
    val tfidf=idfModel.transform(tf)
    
    
            
    val bsCode=getConf.get(Computing.COMPUTING_ID) + "_"+getConf.get(Computing.COMPUTING_BITCH_ID)
//    
//

    val idfPath=getConf.get("default.model.path")+"/"+bsCode
//
    LOG.info("将模型保存到:"+idfPath+IDFModel.getClass.getSimpleName)
    idfModel.write.overwrite().save(idfPath+"/"+IDFModel.getClass.getSimpleName)
//    
    val stringIndex=new StringIndexer().setInputCol("label").setOutputCol("labelIndex").fit(tfidf)
//    
    stringIndex.write.overwrite().save(idfPath+"/"+StringIndexerModel.getClass.getSimpleName)
//    
////    println(stringIndex.labels.)
//    
////    val label=ss.createDataFrame(stringIndex.labels.map { x => (0,x) }.toSeq).toDF("id","label")
//    
////    stringIndex.transform(label).
//    
    val finaltfIdf=stringIndex.transform(tfidf).select("label","labelIndex","features")
//        
//   
    val size=finaltfIdf.select("features").head().getAs[Vector](0).size



    LOG.info("任务执行完毕，将数据导入 table.features，准备下一个任务")

    finaltfIdf.createOrReplaceTempView("features")
    
    result
  }
  
 

  private def deletePath(path:String,overwrite:Boolean):Boolean={
    val fs=FileSystem.get(getConf)
    val fpath=new Path(path)
    if(!overwrite)
      fs.deleteOnExit(fpath)
    
    fs.close()
    
    false
  }
  
  private def store(put: Put) {
    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + output
    println("save log idf to hbase:", tableName)
    HbaseServer.save(put, getConf, tableName)
  }

  private def writeToHdfs(path: String, str: String, overwrite: Boolean = false): Boolean = {

    val seedDir = new Path(path)
    val fs = FileSystem.get(getConf)
    
    try {
      if (!overwrite) {
        fs.deleteOnExit(seedDir)
      }
      val os = fs.create(seedDir)
      os.write(str.getBytes)
      os.flush()
      os.close()
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {

      fs.close()

    }

    true
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

    val output = this.args.get(Computing.OUTPUT_TABLE).asInstanceOf[String]
    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val tableName = bizCode + ":" + output

    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }
}