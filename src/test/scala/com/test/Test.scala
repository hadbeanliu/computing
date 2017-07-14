package com.test

import scala.reflect.internal.Required
import scala.util.Random
import com.google.gson.Gson
import com.recommendengine.compute.utils.TextSplit
import com.recommendengine.compute.api.resource.MiningResource
import org.apache.hadoop.fs.FileSystem
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.File
import com.recommendengine.compute.db.hbase.HbaseServer
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.ml.feature.IDFModel
import scala.collection.mutable.Map
import org.restlet.resource.ClientResource
import scala.io.Source
import java.io.PrintWriter
import scala.collection.mutable.WrappedArray
import org.lionsoul.jcseg.test.JcsegTest
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.conf.ComputingConfiguration
import com.recommendengine.compute.metadata.Computing
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.ml.feature.StringIndexerModel
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory
import com.recommendengine.compute.utils.TextRankKeyWordExtor
import java.util.HashMap
import org.apache.spark.ml.linalg.DenseVector

object Test {

  def main(args: Array[String]): Unit = {

    val ss=SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    
    val sc=ss.sparkContext
    
    val data=sc.textFile("file:///home/hadoop/train/lexicon/customize/lex-main.lex").map { x => {
         val split=x.split("/")
         if(split.length==4)
            (split(0),(split(1),1))
          else ("",("",1))
      } }
    
    data.reduceByKey((x,y)=>(x._1,x._2+y._2)).filter(_._2._2>1).collect().foreach(println)
  }
  def tst(arg: Array[_]) {
    println(arg.length)
  }
}