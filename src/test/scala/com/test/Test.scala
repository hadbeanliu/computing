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

object Test {

 
  def main(args: Array[String]): Unit = {
    
     val st=Source.fromFile("/home/hadoop/train/file")
     val str=new StringBuffer
     st.getLines().foreach { x => str.append(x) }
//    val str="今天，";
//     val str="今日，苹果公司在福州鼓楼区华西北路对一些支持热更新的iOS软件开发者提出了最后通牒，其发布邮件公告称，一些开发者存在“热更新”（即绕过 App Store 审核的更新），因此，苹果要求开发者移除所有相关代码、框架或SDK，限期10天整改，否则将直接下架";
     val jcseg=new JcsegTest
//     jcseg.resetMode(JcsegTaskConfig.NLP_MODE)
//    
//    jcseg.keywords(str.toString())
//    jcseg.tokenize(str)
//    jcseg.sentence(str)
//    jcseg.keyphrase(str)
    jcseg.tokenize(str.toString())
  }
}