package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.similary.SimilaryCaculate
import com.recommendengine.compute.metadata.Computing
import java.util.HashMap

class BaseItemCFRecommender extends ComputingTool with Readable with Writable {

  private val SIMILAR_METHOD = "similar"

  private val TOP_K = "topK"

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE)

    val table = bizCode + ":" + input

    val scan = HbaseServer.buildScan(Array(("behavior", Array(ssCode))), null, null, null)
    HbaseServer.flatGet(table, scan, sc,  x =>
      {

        val cell = x.rawCells()(0)

        val u = Bytes.toString(CellUtil.cloneRow(cell))

        val ratings = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)

        for (rating <- ratings) yield {

          val i = rating.split(Computing.KEY_VALUE_SPLIT_SIGN)(0)
          val v = rating.split(Computing.KEY_VALUE_SPLIT_SIGN)(1).toDouble
          (u, i, v)
        }

      }
    )
    

  }

  def run = {
    val result=new HashMap[String,Any]
    val dataModel = read

    //    val dataModel=sc.textFile("/input/pref.txt", 2).map {x=>{
    //      val field= x.split(Computing.KEY_KEY_SPLIT_SIGN)
    //        (field(0),field(1),field(2).toDouble)
    //    } }
    val similar = if (this.args.get(SIMILAR_METHOD) == null) "cosine" else this.args.get(SIMILAR_METHOD)
    val topK = this.args.get(this.TOP_K).asInstanceOf[String].toInt

    val item2Users = dataModel.groupBy(_._2)
    val numRatingPerItem = item2Users.map(f => (f._1, (f._2.map(_._3).sum / f._2.size, f._2.size)))

    val ratingWithSize = item2Users.join(numRatingPerItem).flatMap(f => {

      f._2._1.map(group => {
        (group._1, group._2, group._3, f._2._2)
      })

    })

    val rating2 = ratingWithSize.keyBy(_._1)

    val ratingPairs = rating2.join(rating2).filter(f => f._2._1._2 < f._2._2._2)

    val tmpVectorsCols = ratingPairs.map(f => {

      val key = (f._2._1._2, f._2._2._2)
      val ex = f._2._1._4._1
      val ey = f._2._2._4._1

      val x = f._2._1._3 - ex
      val y = f._2._2._3 - ey
      val cals = (
        x * y, //x*y
        Math.pow(x, 2), //x*x
        Math.pow(y, 2), //y*y
        x, //x
        y, //y
        f._2._1._4._2, // size x
        f._2._2._4._2 // size y
        )

      (key, cals)

    })
    val vectorsCols = tmpVectorsCols.groupByKey().map(data => {
      val key = data._1
      val cals = data._2

      val size = cals.size

      val sumXY = cals.map(_._1).sum
      val sumXX = cals.map(_._2).sum
      val sumYY = cals.map(_._3).sum
      val sumX = cals.map(_._4).sum
      val sumY = cals.map(_._5).sum
      val numX = cals.map(_._6).max
      val numY = cals.map(_._7).max

      (key, size, sumXY, sumXX, sumYY, sumX, sumY, numX, numY)
    })

    val tmpSimilaris = vectorsCols.map(f => {
      val similary = similar match {
        case "cosine"  => SimilaryCaculate.cosineSimilary(f._3, f._4, f._5)
        case "jaccard" => SimilaryCaculate.jaccard(f._2, f._8, f._9)
        case other     => SimilaryCaculate.pearsonSimilary(f._3, f._6, f._7, f._4, f._5)
      }
      (f._1._1, (f._1._2, similary))

    })

    val totalSimilaris = tmpSimilaris.map(f => (f._2._1, (f._1, f._2._2))).union(tmpSimilaris)

    val similaris = totalSimilaris.groupByKey().flatMap(f => {
      f._2.map(tmp => (f._1, (tmp._1, tmp._2))).toList.sortWith((a, b) => a._2._2 > b._2._2).take(topK)

    })
// 预测用户对物品的评分和喜好程度
//    val inverRating=dataModel.map(f=>(f._2,(f._1,f._3)))
//    
//    val toPredict=similaris.join(inverRating).map(f=>{
//      ((f._2._2._1,f._2._1._1),(f._2._1._2,f._2._1._2*f._2._2._2))
//    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._2/x._2._1))

    val item2ItemsSimilars = similaris.groupByKey()
    item2ItemsSimilars.cache()
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    write[(String, Iterable[(String, Double)])](item2ItemsSimilars, op => {
      val put = new Put(op._1.getBytes)
      val buff = new StringBuffer
      op._2.foreach(kv => buff.append(kv._1).append(Computing.KEY_VALUE_SPLIT_SIGN).append(kv._2).append(Computing.VALUE_KEY_SPLIT_SIGN))
      put.add("result".getBytes, ssCode.getBytes, buff.toString().getBytes)
      (new ImmutableBytesWritable, put)
    })
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