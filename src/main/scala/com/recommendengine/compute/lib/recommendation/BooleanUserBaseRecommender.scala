package com.recommendengine.compute.lib.recommendation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.recommendengine.compute.lib.similary.SimilaryCaculate
import org.apache.hadoop.hbase.client.Result
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import java.util.HashMap

/**
 * *
 *
 * @author 刘海斌
 *
 * 基于近邻的基于用户的协同过滤算法，
 *
 * @param K是和用户最相近的几个用户，默认50个
 * @param similary是和用户相度为多少的量
 *
 *
 */

class BooleanUserBaseRecommender(k: Int,similary:String) extends ComputingTool {

  
  
    def read ={
      
      null
    }  
  def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put))={
    
  }


  def run() = {
    
    val result=new HashMap[String,Any]
    
    val ratings: RDD[Rating]=null
    //获得(user,item,value)
    val userRatings = ratings.map { x => (x.user, x.product, x.rating) }.groupBy(_._1).flatMap(f => f._2.toList.sortWith((x, y) => x._2 < y._2))

    //(user,item.value)=>(user,List((user,item,value)))
    val user2Items = userRatings.groupBy(_._1)
    // (user,List((user,item,value)))=>(user,List.size)
    val numUserRatings = user2Items.map(f => (f._1, f._2.size))

    //(user,item)
    val user2ItemsWithSize = user2Items.join(numUserRatings).flatMap(
      joined => {
        joined._2._1.map(f => (joined._1, f._2,f._3, joined._2._2))
      })
      //(item,List((user,item,value,size)))
    val ratings2 = user2ItemsWithSize.keyBy(_._2)

    //(itemId,(u,item,value,size))=>(item,(u1,item,value,size1),(u2,item,value,size2)),并过滤重复的元素
    val rating2Rating = ratings2.join(ratings2).filter(f => f._2._1._1 < f._2._2._1)

    //(itemId,(u1,item,value,size1),(u2,item,value,size2))=>((u1,u2),(item,value,size1,size2))
    val user2user = rating2Rating.map(group => {
      val key = (group._2._1._1, group._2._2._1)

      val stats = (group._1, group._2._1._3, group._2._2._3,group._2._1._4,group._2._2._4)

      (key, stats)
    })

    // ((u1,u2),(itemId,v1,,v2,s1,s2))=>((u1,u2),List((itemId,v1,v2,s1,s2)))=>(u1,(u2,similary))
    val similaryVec = user2user.groupByKey().map(f => {
      val key = f._1
      val xAy = f._2.size
      val xy=f._2.map(f=>f._2*f._3).sum
      val xx = f._2.map(x=>math.pow(x._2, 2)).sum
      val yy = f._2.map(x=>math.pow(x._3, 2)).sum
      val lengthX=f._2.map(_._4).max
      val lengthY=f._2.map(_._5).max
      //val similary = SimilaryCaculate.jaccard(xAy, lengthX, lengthY)
      val similary=SimilaryCaculate.cosineSimilary(xy, xx, yy)
      (key._1, (key._2, similary))
    })
    //(u1,(u2,similary))=>(u2,(u1,similary))
    val otherHalf = similaryVec.map(f => (f._2._1, (f._1, f._2._2)))

    val similarys = similaryVec ++ otherHalf

    val topKSimilaryUser = similarys.groupByKey().flatMap(f => f._2.toList.sortWith((x, y) => x._2 > y._2).take(k).map(z => (f._1, (z._1, z._2))))

    val user2ItemAndPref = userRatings.map(f => (f._1, (f._2, f._3)))
    
    val statistics = topKSimilaryUser.join(user2ItemAndPref).map(x => ((x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2 * x._2._1._2)))

    val predict = statistics.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(f=>(f._1,f._2._2))
    
    result    
  }

}

object BooleanUserBaseRecommender {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setMaster("local[*]").setAppName("BooleanUserCF")
    val sc = new SparkContext(sconf)
    val ratings = sc.textFile("file:///home/hadoop/result/userPrefs.txt", 2).map { x =>
      x.split(":") match {
        case Array(user, item, rating) => Rating(user.toInt, item.toInt, rating.toDouble)
      }
    }

    val booUserBaseRecommend = new BooleanUserBaseRecommender(5,"jaccard")
//    booUserBaseRecommend.run()
    
  }

}
