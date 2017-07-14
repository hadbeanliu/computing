package com.recommendengine.compute.api.resource

import java.net.URLDecoder

import scala.collection.mutable.Map

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.util.Bytes

import com.google.gson.Gson
import com.recommendengine.compute.api.RecServer
import com.recommendengine.compute.api.model.WebPage
import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.TextSplit

import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuilder
import javax.ws.rs.core.MediaType
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileNotFoundException
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import com.recommendengine.compute.utils.TextRankKeyWordExtor
import java.util.HashMap
import org.apache.spark.sql.functions.{ udf }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.Column
import com.recommendengine.compute.utils.NumberFormat
import com.recommendengine.compute.utils.NumberFormat
import java.util.ArrayList

object MiningResource {

  val conf = ComputingConfiguration.createWithHbase

  val models = Map[String, Any]()

  var idfModel = Map[String, Any]()

  var nnz = Map[Double, DenseVector]()

  var label = Map[String, Array[String]]()

  def toTF(str: String): Unit = {

  }

  //  def toTFIDF(idf: Vector, v: Vector): Vector = {
  //
  //    val n = v.size
  //
  //    v match {
  //      case SparseVector(size, indices, values) => {
  //        val nnz = indices.size
  //        val newValues = new Array[Double](nnz)
  //        var k = 0
  //        while (k < nnz) {
  //          newValues(k) = values(k) * idf(indices(k))
  //          k += 1
  //        }
  //        Vectors.sparse(n, indices, newValues)
  //      }
  //
  //      case DenseVector(values) => {
  //        var j = 0
  //        val newValues = new Array[Double](n)
  //        while (j < n) {
  //          newValues(j) = v(j) * idf(j)
  //          j += 1
  //        }
  //        Vectors.dense(newValues)
  //      }
  //
  //      case other => throw new UnsupportedOperationException(
  //        s"Only sparse and dense vectors are supported but got ${other.getClass}.")
  //    }
  //
  //  }

}

@Path(value = "/mining")
class MiningResource extends AbstractResource {

  @GET
  @Path(value = "/test")
  def test(): String = {

    return "here are you"
  }

  @POST
  @Path("/data-list")
  def dataList(taskConf: String): String = {

    val encode = URLDecoder.decode(taskConf.substring(9), "UTF-8")

    null
  }

  @POST
  @Path("/classify")
  @Produces(Array(MediaType.TEXT_HTML))
  def classify(text: String, @QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String, @QueryParam("model") modelClass: String): Any = {

    println("comeing here!!!!!!")
    require(bizCode != null && ssCode != null, s"参数不足或者为空!!=>[biz_code=$bizCode],[ss_code=$ssCode],[model=$modelClass]")
    require(text != null, s"内容不能为空!!=>$text")

    val ss = SparkSession.builder().master("local[*]").getOrCreate()
    val bsCode = bizCode + "_" + ssCode

    println(bsCode)
    val conf = MiningResource.conf

    if (MiningResource.models.get(bsCode) == None) {
      val model = NaiveBayesModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + NaiveBayesModel.getClass.getSimpleName)
      MiningResource.models.put(bsCode, model)
    }
    if (MiningResource.idfModel.get(bsCode) == None) {

      MiningResource.idfModel.put(bsCode, IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName))
    }

    if (MiningResource.label.get(bsCode) == None) {
      val stringIndex = StringIndexerModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + StringIndexerModel.getClass.getSimpleName)
      MiningResource.label.put(bsCode, stringIndex.labels)
    }

    val labels = MiningResource.label.get(bsCode).get

    val idfModel = MiningResource.idfModel.get(bsCode).get
    val predictModel = MiningResource.models.get(bsCode).get
    //    if (predictModel == None || idfModel == None || labels==None)
    //      throw new NullPointerException("尚未训练模型")

    val splitWords = TextSplit.process(text)
    val wordSize = splitWords.size
    val data = ss.createDataFrame(Seq((0, splitWords))).toDF("id", "words")

    val tf = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").transform(data)

    //    val func = udf {
    //      vec:Vector=>
    //         val v=vec.toSparse
    //         val size=v.values.count { x => x>0 }
    //         val values=v.values.map { x => x/size }
    //         
    //         new SparseVector(vec.size,v.indices,values)
    //    }
    //    val withTf=tf.withColumn("rowFeatures",func(tf("rowFeatures")))

    val tfidf = idfModel.asInstanceOf[IDFModel].transform(tf)

    val predicts = predictModel.asInstanceOf[NaiveBayesModel].transform(tfidf).select("probability").collect()(0).getAs[DenseVector](0).toArray

    val reduce = predicts.map { x => 1 / Math.abs((Math.log10(x) + 5)) }
    val count = reduce.sum

    //    val maxLog=predicts.max
    //    val size=predicts.size
    //    var i=0
    //    while(i<size){
    //      
    //      predicts(i)=Math.exp(predicts(i)-maxLog)
    //      i += 1
    //    }
    //    val proSum=predicts.sum
    //    i=0
    //    while(i<size){
    //      
    //      predicts(i)=predicts(i)/proSum
    //      i += 1
    //    }
    //    
    val result = for (i <- 0 until labels.length) yield (labels(i), reduce(i) / count)

    result.filter(_._2 > 0).sortWith((x, y) => x._2 > y._2).toArray.mkString(",")
  }

  @POST
  @Path("/extractkw")
  @Produces(Array(MediaType.TEXT_HTML))
  def extractKeyWords(text: String, @QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String): String = {

    require(text != null, "text cant be null")
    println("comeing here!!!!!!")
    require(bizCode != null && ssCode != null, s"参数不足或者为空!!=>[biz_code=$bizCode],[ss_code=$ssCode]")
    require(text != null, s"内容不能为空!!=>$text")

    val ss = SparkSession.builder().master("local[*]").getOrCreate()
    val bsCode = bizCode + "_" + ssCode
    val conf = MiningResource.conf
    val extractor = new TextRankKeyWordExtor
    if (MiningResource.idfModel.get(bsCode) == None) {

      MiningResource.idfModel.put(bsCode, IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName))
    }
    val idf = MiningResource.idfModel.get(bsCode).get.asInstanceOf[IDFModel].idf
    val gson=new Gson
    val words = gson.fromJson(text, classOf[Array[String]])
    val textRank = extractor.getKeyWordWithScore(words).filter(_._1.length() > 1).map(f => (f._1, Float.float2double(f._2)))
    val hashingTf = new org.apache.spark.mllib.feature.HashingTF(262144)
    val tf = hashingTf.transform(words)

    val format = new NumberFormat();
    val normailize = (data: Array[(String, Double)]) => {
      val max = data.map(_._2).max
      val min = data.map(_._2).min
      val maxmin = max - min
      data.map(f => (f._1, 10 * (f._2 - min) / maxmin))
    }
//     val result=new HashMap[String,Weight]
//    words.distinct.foreach { x =>
//      {
//        val index = hashingTf.indexOf(x)
//        val t = tf(index)
//        val tfidf = t * idf(index)
//        result.put(x,Weight(x, t, tfidf))
//      }
//    }


    
        val result = new HashMap[String, String]
    
        val withIDF = normailize(textRank.map { w =>
          {
            val index = hashingTf.indexOf(w._1)
    
            (w._1, w._2 * idf(index))
    
          }
        }).sortWith((x,y)=>x._2>y._2).take(10).map(f=>(f._1,format.format(f._2))).mkString("|")
    
        val tfidf = normailize(textRank.map { w =>
          {
            val index = hashingTf.indexOf(w._1)
    
            (w._1, tf(index) * idf(index))
    
          }
        }).toMap
    
        val andTFIDF = normailize(textRank).map(w => {
          (w._1, (tfidf(w._1) + w._2)/2)
    
        }).sortWith((x,y)=>x._2>y._2).take(10).map(f=>(f._1,format.format(f._2))).mkString("|")
    
        result.put("withIDF", withIDF)
        result.put("andTFIDF", andTFIDF)
    gson.toJson(result)
  }

  def getOrElseMeaningFulUser(text: String, @QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String, @QueryParam("model") modelClass: String): Any = {

  }

  @POST
  @Path("/class")
  @Produces(Array(MediaType.TEXT_HTML))
  def classify2(text: String, @QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String): Any = {

    println("comeing here!!!!!!")
    require(bizCode != null && ssCode != null, s"参数不足或者为空!!=>[biz_code=$bizCode],[ss_code=$ssCode]")
    require(text != null, s"内容不能为空!!=>$text")

    val ss = SparkSession.builder().master("local[*]").getOrCreate()
    val bsCode = bizCode + "_" + ssCode

    val words = TextSplit.process(text)
    val wordSize = TextSplit.process(text).size

    val hashingTf = new org.apache.spark.mllib.feature.HashingTF(262144)
    var tf = hashingTf.transform(words).toSparse
    val func = (v: org.apache.spark.mllib.linalg.SparseVector) => {

      val size = v.values.count { x => x > 0 }
      val values = v.values.map { x => x / size }

      new SparseVector(v.size, v.indices, values)
    }

    val withtf = func(tf)

    val conf = MiningResource.conf

    if (MiningResource.models.get(bsCode) == None) {
      val model = NaiveBayesModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + NaiveBayesModel.getClass.getSimpleName)
      MiningResource.models.put(bsCode, model)
    }
    if (MiningResource.idfModel.get(bsCode) == None) {

      MiningResource.idfModel.put(bsCode, IDFModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + IDFModel.getClass.getSimpleName))
    }

    if (MiningResource.label.get(bsCode) == None) {
      val stringIndex = StringIndexerModel.load(conf.get("default.model.path") + "/" + bsCode + "/" + StringIndexerModel.getClass.getSimpleName)
      MiningResource.label.put(bsCode, stringIndex.labels)
    }

    val labels = MiningResource.label.get(bsCode).get

    val idfModel = MiningResource.idfModel.get(bsCode).get
    val predictModel = MiningResource.models.get(bsCode).get.asInstanceOf[NaiveBayesModel]
    //    if (predictModel == None || idfModel == None || labels==None)
    //      throw new NullPointerException("尚未训练模型")   
    if (MiningResource.nnz.size == 0) {
      val aggregate = ss.read.parquet(conf.get("default.model.path") + "/" + bsCode + "/" + "aggregate")

      aggregate.rdd.map { x => (x.getDouble(0), x.getAs[Vector](1)) }.collect().foreach(f => {
        val values = f._2.toDense.values
        val size = values.size
        var k = 0
        while (k < size) {
          if (values(k) > 0)
            values(k) = 1 / values(k)
          k += 1
        }

        MiningResource.nnz.put(f._1, new DenseVector(values))
      })
    }

    val pi = predictModel.pi
    val theta = predictModel.theta
    val idf = idfModel.asInstanceOf[IDFModel].idf
    val numRows = theta.numRows

    var k = 0
    val prop = new Array[Double](theta.numRows)
    for (v <- theta.rowIter) {
      var result = 0d
      val v1 = MiningResource.nnz.get(k.toDouble).get
      v match {
        case v: SparseVector => {
          val tfindices = withtf.indices
          val tfSize = tfindices.size
          var z = 0
          while (z < tfSize) {
            val index = tfindices(z)
            result += v1.values(index) * withtf.values(index) * v.values(index) * idf(index)
            z += 1
          }

        }
        case v: DenseVector => {
          val tfindices = withtf.indices
          val tfSize = tfindices.size
          var z = 0
          while (z < tfSize) {

            val index = tfindices(z)
            result += v1.values(index) * withtf.values(z) * v.values(index) * idf(index)

            z += 1
          }
        }
        case _ => result = 0
      }
      prop(k) = result
      k += 1
      result
    }

    val firstResult = for (index <- 0 until pi.size) yield prop(index) * pi(index)

    val sum = firstResult.sum

    val predicts = firstResult.map { x => x / sum }

    println("all the same===>>", predicts.sum)
    //    val func = udf {
    //      vec:Vector=>
    //         val v=vec.toSparse
    //         val size=v.values.count { x => x>0 }
    //         val values=v.values.map { x => x/size }
    //         
    //         new SparseVector(size,v.indices,values)
    //    }
    //    val withTf=tf.withColumn(func(new Column("rowFeatures")))

    //    val tfidf = idfModel.asInstanceOf[IDFModel].transform(tf)
    //    val predicts = predictModel.asInstanceOf[NaiveBayesModel].transform(tfidf).select("probability").collect()(0).getAs[DenseVector](0).toArray
    //    
    //    val reduce = predicts.map { x => 1 / Math.abs((Math.log10(x) + 5)) }
    //    val count = reduce.sum
    //    val maxLog=predicts.max
    //    val size=predicts.size
    //    var i=0
    //    while(i<size){
    //      
    //      predicts(i)=Math.exp(predicts(i)-maxLog)
    //      i += 1
    //    }
    //    val proSum=predicts.sum
    //    i=0
    //    while(i<size){
    //      
    //      predicts(i)=predicts(i)/proSum
    //      i += 1
    //    }
    //    
    val result = for (i <- 0 until labels.length) yield (labels(i), predicts(i))

    result.filter(_._2 > 0).sortWith((x, y) => x._2 > y._2).toArray.mkString(",")
  }

}
case class Weight(word: String, idf: Double, tfidf: Double)