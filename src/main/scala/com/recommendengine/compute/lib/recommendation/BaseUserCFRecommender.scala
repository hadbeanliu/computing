package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.similary.SimilaryCaculate
import com.recommendengine.compute.metadata.Computing
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import java.util.HashMap

class BaseUserCFRecommender extends ComputingTool {
  
  
  private val DEFAULT_INPUT="user_item_score_list"
  private val DEFAULT_OUTPUT="user_item_rec_list"

  def read= {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val input = this.args.get(Computing.INPUT_TABLE)

    val table = bizCode + ":" + input

    val scan = HbaseServer.buildScan(Array(("bhv", Array(ssCode))), null, null, null)
    var data = HbaseServer.flatGet(table, scan, sc,  x =>
          {
    
            val cell = x.rawCells()(0)
    
            val u = Bytes.toString(CellUtil.cloneRow(cell))
    
            val ratings = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)
    
            for (rating <- ratings) yield {
              val field=rating.split(Computing.KEY_VALUE_SPLIT_SIGN)
              val i = field(0)
              val v = field(1).toDouble
              (u, i, v)
            }
    
          }
        )
    data

  }

  def run() ={
      val returns=new HashMap[String,Any]
         val dataModel = read 
//    val dataModel = sc.textFile("/input/pref.txt", 2).map { x =>
//      {
//        //        println(x)
//        val field = x.split(":")
//        (field(0), field(1), field(2).toDouble)
//      }
//    }

    //    val ratingWithSize=dataModel.groupBy(_._2).map(f=>{
    //      (f._1,(f._2.map(_._3).sum/f._2.size,f._2.size))
    //    })

    val user2ManyItem = dataModel.groupBy(_._1)

    val perUserWithSize = user2ManyItem.map(f => (f._1, f._2.size))

    val ratingWithSize = perUserWithSize.join(user2ManyItem).flatMap(f => f._2._2.map(group => (group._1, group._2, group._3, f._2._1)))

    val rating2 = ratingWithSize.keyBy(_._2)

    val ratingPairs = rating2.join(rating2).filter(f => f._2._1._1 < f._2._2._1)

    val tmpVectors = ratingPairs.map(data => {

      val key = (data._2._1._1, data._2._2._1)

      val cals = (
        data._2._1._3 * data._2._2._3, //rating1*rating2
        data._2._1._3, //rating1
        data._2._2._3, //rating2
        data._2._1._4,
        data._2._2._4,
        Math.pow(data._2._1._3, 2), //rating1*rating1
        Math.pow(data._2._2._3, 2) //rating2*rating2
        )
      (key, cals)
    })

    val tmpcals = tmpVectors.groupBy(_._1).map(f => {
      val key = f._1
      val sizeXY = f._2.size
      val sumXY = f._2.map(_._2._1).sum
      val sumX = f._2.map(_._2._2).sum
      val sumY = f._2.map(_._2._3).sum
      val sizeX = f._2.map(_._2._4).max
      val sizeY = f._2.map(_._2._5).max
      val sumXX = f._2.map(_._2._6).sum
      val sumYY = f._2.map(_._2._7).sum

      (key, (sizeXY, sumXY, sumX, sumY, sizeX, sizeY, sumXX, sumYY))
    })

    val inverseVectorCalcs = tmpcals.map(f => {
      ((f._1._2, f._1._1), (f._2._1, f._2._2, f._2._4, f._2._3, f._2._6, f._2._5, f._2._8, f._2._7))
    })

    val allCals = inverseVectorCalcs.union(tmpcals)

    val tmpsimilar = inverseVectorCalcs.map(f => {
      val key = f._1
      val (sizeXY, sumXY, sumX, sumY, sizeX, sizeY, sumXX, sumYY) = f._2

      val cosSim = SimilaryCaculate.cosineSimilary(sumXY, sumXX, sumYY)
      (key._1, (key._2, cosSim))

    })

    val topK = if (this.args.get("topK") != null) this.args.get("topK").asInstanceOf[String].toInt else 50

    val similar = tmpsimilar.groupByKey().flatMap(f => {
      f._2.map(tmp => (f._1, tmp)).toList.sortWith((x, y) => x._2._2 > y._2._2).take(topK)

    })

    similar.take(10).foreach(println)

    //    val tmp\
    val ratingInverse = dataModel.map(f => (f._1, (f._2, f._3)))

    val statistis = ratingInverse.join(similar).map(f => {
      ((f._2._2._1, f._2._1._1), (f._2._2._2, f._2._2._2 * f._2._1._2))
    })

    val predictResult = statistis.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(f => (f._1, f._2._2 / f._2._1))

    val finalPredict = predictResult.map(f => (f._1._1, (f._1._2, f._2)))

    val item2ItemsSimilars = finalPredict.groupByKey()
    item2ItemsSimilars.cache()

    val result = finalPredict.groupByKey().flatMap(f => {
      f._2.map(tmp => (f._1, tmp)).toList.sortWith((a, b) => a._2._2 > b._2._2).take(topK)

    }).groupByKey()

    //    result.take(50).foreach(println)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    write[(String, Iterable[(String, Double)])](result, op => {
      val put = new Put(op._1.getBytes)
      val buff = new StringBuffer
      op._2.foreach(kv => buff.append(kv._1).append(Computing.KEY_VALUE_SPLIT_SIGN).append(kv._2).append(Computing.VALUE_KEY_SPLIT_SIGN))
      put.add("result".getBytes, ssCode.getBytes, buff.toString().getBytes)
      (new ImmutableBytesWritable, put)
    })
    
    returns
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