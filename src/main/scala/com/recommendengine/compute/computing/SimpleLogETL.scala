package com.recommendengine.compute.computing

import com.recommendengine.compute.utils.LogFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ udf }
import org.apache.spark.sql.Row

object SimpleLogETL {
  
  
  def main(args: Array[String]): Unit = {
    
    val topics="news"
    
    val host="master:2181"
    val string_format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent";
    //    import ss.implicits._
    //    import org.apache.spark.sql.functions.window
    // 192.168.16.109 - - [08/Aug/2017:00:00:06 +0800] "POST /HWEB/item/saveCrawler HTTP/1.1" 200 4
    //    val Array(zkquery,group,topics,numThread) =args
    val format = LogFormat.getInstance(string_format)
    
    val ss=SparkSession.builder().appName("ETL").master("local[*]").getOrCreate()
    
    val df=ss.read.format("kafka").option("kafka.bootstrap.servers", host).option("subscribe", topics).load()
    val line=df.selectExpr("CAST( key AS STRING)","CAST(value AS STRING)").filter { x => x.getAs[String]("value").contains("getDetail") }
    val stringTOStruct = udf{
      row:Row=>{
        
      }
      
    }
    val data3=line.rdd.map { x => x.get(2) }
    
  }
}