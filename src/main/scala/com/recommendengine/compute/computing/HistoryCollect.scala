package com.recommendengine.compute.computing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import com.recommendengine.compute.utils.LogFormat
import com.google.gson.Gson
import com.recommendengine.compute.db.hbase.HbaseServer
import java.util.regex.Pattern
import org.apache.hadoop.hbase.client.Put
import java.util.Date
import java.util.Formatter.DateTime
import java.text.DateFormat
import sun.java2d.pipe.SpanShapeRenderer.Simple
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Get
import java.util.ArrayList
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.ConnectionManager
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.TableName
import org.apache.spark.streaming.Duration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Append
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.CellUtil

object HistoryCollect {

  def main(args: Array[String]): Unit = {
    //    val ss = SparkSession.builder().appName("history-collect").master("local[*]").getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val MAX_VALUE_TIME = 3000000000000000L
    val string_format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent";
    //    import ss.implicits._
    //    import org.apache.spark.sql.functions.window
    // 192.168.16.109 - - [08/Aug/2017:00:00:06 +0800] "POST /HWEB/item/saveCrawler HTTP/1.1" 200 4
    //    val Array(zkquery,group,topics,numThread) =args
    val format = LogFormat.getInstance(string_format)

    val topics = "news"
    val numThread = "1"
    println(s"{$topics}--$numThread")
    val sconf = new SparkConf().setMaster("local[*]").setAppName("kafkaWordCount")

    val topicMap: Map[String, Int] = topics.split(",").map((_, numThread.toInt)).toMap

    val ssc: StreamingContext = new StreamingContext(sconf, Seconds(10))
    val brv = ssc.sparkContext.broadcast(format)
    ssc.checkpoint("checkpoint")
    //    ssc.union(streams)

    val lines = KafkaUtils.createStream(ssc, "master:2181", "test-consumer-group", topicMap, StorageLevel.MEMORY_AND_DISK_2)

    val query = lines.map(_._2).filter(_.contains("getDetail")).mapPartitions(it => {
      val model = brv.value
      val p = Pattern.compile("\\d{9,}")
      val dateformat = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z")
      for (v <- it) yield {
        val map = model.parse(v)
        val u = map.get("remote_addr")
        val time = dateformat.parse(map.get("time_local")).getTime

        val rq = map.get("request")
        val m = p.matcher(rq)
        val iid = if (m.find()) m.group() else null
        (iid, (u, "v", time))
      }

    }).groupByKeyAndWindow(Seconds(10))
    //    query.checkpoint(Minutes(5))
    //加载所需的文章内容,生成实时用户画像
    //    val items=query.map(s=>(MAX_VALUE_TIME-s._2._1.toLong).toString()).mapPartitions(ids=>{
    //      val gets= for(id<-ids)yield{
    //        val get=new Get(id.getBytes)
    //        get.addFamily("kw".getBytes)
    //        get
    //      }
    //      val conn=ConnectionFactory.createConnection()
    //      val table = conn.getTable(TableName.valueOf("headlines:item_meta_table"))
    //      val rs=table.get(gets.toList)
    //      val itemWithTags=rs.filter { x => !x.isEmpty() }.map { x => (Bytes.toString(x.getRow),x.listCells().map { cell => Bytes.toString(CellUtil.cloneQualifier(cell)) }.toArray) }
    //       table.close()
    //       conn.close()
    //       itemWithTags.toIterator
    //    })

    //    val itemWithTags=query.map(x=>(MAX_VALUE_TIME - x._2._1.toLong).toString).
    //保存数据到数据库,更新用户画像
    import scala.collection.JavaConversions._

    import org.apache.spark.sql.functions.{ udf }
    def idReserve = (id: String) => { (MAX_VALUE_TIME - id.toLong).toString }

    query.foreachRDD(rdd => {
      val data = rdd.collect()
      if (data.length > 0) {

        val getItemsKW = data.map(act => idReserve(act._1)).map { x =>
          {
            val get = new Get(x.getBytes)
            get.addFamily("kw".getBytes)
          }
        }

        val conf = HBaseConfiguration.create()
        val conn = ConnectionFactory.createConnection(conf)
        val table = conn.getTable(TableName.valueOf("headlines:item_meta_table"))
        val uTable = conn.getTable(TableName.valueOf("headlines:user_msg_table"))
        //        val hisTable=conn.getTable(TableName.valueOf("headlines:his_act_record"))

        try {
          val items = table.get(getItemsKW.toList)

          val itemWithNum = data.map { x => (x._1, x._2.size) }
          for ((iid, num) <- itemWithNum) {
            val increment = new Increment(idReserve(iid).getBytes)
            increment.addColumn("s".getBytes, "v".getBytes, num)
            table.increment(increment)
          }

          val idWithTag = items.filter { x => !x.isEmpty() }.map { row => (Bytes.toString(row.getRow), row.listCells().map { x => Bytes.toString(CellUtil.cloneQualifier(x)) }.toArray) }
          val itemMaps = idWithTag.toMap
          //      itemMaps.get(idReserve)
          val appends = new ArrayList[Append]
          val dayMills = 1000 * 24 * 60 * 60

          for (
            (iid, uGroup) <- data if (itemMaps.get(idReserve(iid)) != None)
          ) {
            val tags = itemMaps.get(idReserve(iid)).get
            val newG = uGroup.groupBy(_._1).map { f => f._2.last }
            for ((u, v, t) <- newG) {
              val d = t / dayMills
              val q = v + ":" + d
              val testGet = new Get(u.getBytes)
              testGet.addColumn("bhv".getBytes, q.getBytes)
              if (!uTable.exists(testGet)) {
              
                val put = new Put(u.getBytes)
                //              val apd = new Append(u.getBytes)
                println("put to bhv")
                put.addColumn("bhv".getBytes, q.getBytes, iid.getBytes)
                put.setTTL(90 * dayMills)
                uTable.put(put)
                
              } else {
                val append = new Append(u.getBytes)
                //              val apd = new Append(u.getBytes)
                println("append to bhv")
                append.add("bhv".getBytes, q.getBytes, ("," + iid).getBytes)
                uTable.append(append)
              }
              val increment = new Increment(u.getBytes)
              for (tag <- tags) {
                println(s"..........................$u  $v   $t    $tag")
                increment.addColumn("g".getBytes, tag.getBytes, 1)
              }
              println(s"increment :$u  $v  $t")
              uTable.increment(increment)
              //              uTable.append(apd)
            }

          }

        } catch {
          case e1: NullPointerException => e1.printStackTrace()
          case e2: Exception => e2.printStackTrace()
        } finally {
          table.close()
          uTable.close()
          conn.close()
        }

      }
    })

    //    val query2 = query.mapPartitions(acts => {
    //
    //      val dayMills = 1000 * 24 * 60 * 60
    //      val reserveId = acts.map(act => idReserve(act._1))
    //
    //      val getItemsKW = reserveId.map { x =>
    //        {
    //          val get = new Get(x.getBytes)
    //          get.addFamily("kw".getBytes)
    //        }
    //      }
    //      println("getItem size...." + getItemsKW.toList.size)
    //      val conf = HBaseConfiguration.create()
    //      val conn = ConnectionFactory.createConnection(conf)
    //      val table = conn.getTable(TableName.valueOf("headlines:item_meta_table"))
    //      //      val get1=new Get("982918480998920".getBytes)
    //      //      get1.addFamily("kw".getBytes)
    //      //      val result=table.get(get1)
    //
    //      //      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>",result.isEmpty())
    //      //      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>",result.getFamilyMap("kw".getBytes).size)
    //      val uTable = conn.getTable(TableName.valueOf("headlines:user_msg_table"))
    //      try {
    //        val items = table.get(getItemsKW.toList)
    //        acts.foreach(f => println("??????????????????????????", f._1, f._2.size))
    //        val itemWithNum = acts.map { x => (x._1, x._2.size) }
    //        println("itemWithNum..........", itemWithNum.size)
    //        for ((iid, num) <- itemWithNum) {
    //
    //          val increment = new Increment(idReserve(iid).getBytes)
    //          increment.addColumn("s".getBytes, "v".getBytes, num)
    //          table.increment(increment)
    //        }
    //
    //        val idWithTag = items.filter { x => !x.isEmpty() }.map { row => (Bytes.toString(row.getRow), row.listCells().map { x => Bytes.toString(CellUtil.cloneQualifier(x)) }.toArray) }
    //        val itemMaps = idWithTag.toMap
    //        //      itemMaps.get(idReserve)
    //        val appends = new ArrayList[Append]
    //        for (
    //          (iid, uGroup) <- acts if (itemMaps.get(idReserve(iid)) != None)
    //
    //        ) {
    //          val tags = itemMaps.get(idReserve(iid)).get
    //          for ((u, v, t) <- uGroup) {
    //            val apd = new Append(u.getBytes)
    //
    //            apd.add("bhv".getBytes, (v + ":" + dayMills).getBytes, (iid + ",").getBytes)
    //            apd.setTTL(90 * dayMills)
    //            val increment = new Increment(u.getBytes)
    //            for (tag <- tags) {
    //              println(s"..........................$u  $v   $t    $tag")
    //              increment.addColumn("kw".getBytes, tag.getBytes, 1)
    //            }
    //            println(s"increment :$u  $v  $t")
    //            uTable.increment(increment)
    //            uTable.append(apd)
    //          }
    //
    //        }
    //
    //      } catch {
    //        case e1: NullPointerException => e1.printStackTrace()
    //        case e2: Exception => e2.printStackTrace()
    //      } finally {
    //        table.close()
    //        uTable.close()
    //        conn.close()
    //      }
    //
    //      //      val userAndTags=acts.
    //      //      for((ITEM,UGroup,TAGS)<-userAndTags){
    //      //        
    //      //      }
    //      acts.map { x => (x._1, x._2.size) }
    //    })

    //    query2.foreachRDD(rdd => {
    //
    //      println(rdd.count(), ": in rdd" + rdd.id)
    //
    //    })

    //    lines.count().reduce(_ + _).foreachRDD(rdd=>println(rdd.count()))
    //    val word=lines.flatMap(_._2.split(" ").map { x => (x,1) })

    //    word.reduceByKeyAndWindow(_ + _, Minutes(1)).foreachRDD(rdd=>rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()

  }

  def structStreaming() {

    //    val ss = SparkSession.builder().appName("history-collect").master("local[*]").getOrCreate()
    //    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //    
    //    import ss.implicits._
    //    import org.apache.spark.sql.functions.window

    //    val userSchema = new StructType().add("name", "string").add("age", "int").add("time", "long")
    //
    //    val lines = ss.readStream.text("file:///home/hadoop/ds/test")
    //   
    //    val word = lines.as[String].flatMap(_.split(" "))
    //    
    //    val query = word.groupBy(window($"timestamp","10 minute","5 minute"),$"word").count().writeStream.outputMode("complete").format("console").start()
    //    
    ////      
    //    query.awaitTermination()

    //    val query = word.writeStream.

    //    word.groupBy("value").count().writeStream.foreach(new ForeachWriter[Row]() {
    //      def open(partitionId: Long, version: Long): Boolean = {
    //        false
    //      }
    //      def process(value: Row): Unit = {
    //
    //      }
    //      def close(errorOrNull: Throwable): Unit = {
    //
    //      }
    //
    //    })
    //
    //    val query = word.groupBy("value").count().writeStream.outputMode("append").format("console").start()
    //
    //    query.awaitTermination()

  }
}
case class Action(u: String, timestamp: Long, item: String, actType: String)
case class User(name: String, age: Int, time: Long)