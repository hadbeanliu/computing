package com.test

import java.util.ArrayList
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.client.Delete
import java.util.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import com.recommendengine.compute.utils.HtmlParser

object HbaseTest {

  def main(args: Array[String]): Unit = {
    //        val conn=HConnectionManager.createConnection(HBaseConfiguration.create())
    //        
    //        val table=conn.getTable("headlines:item_meta_table".getBytes)

    //        val ss=SparkSession.builder().config("master", "local[*]").appName("test").getOrCreate()

    //        val data = ss.sparkContext.wholeTextFiles("file:///home/hadoop/result/train")
    countBySelect
    //          countBySelect()
    //          HbaseServer.clearTable("headlines:item_meta_table")
    //    
    //    val delete=new Delete("2017011619000064".getBytes)
    ////    delete.deleteFamily("kw".getBytes)
    //    table.delete(delete)

  }

  def countBySelect() {
    val conn = HConnectionManager.createConnection(HBaseConfiguration.create())

    val scan = new Scan
    scan.addColumn("f".getBytes, "lb".getBytes)

    val table = conn.getTable("headlines:item_meta_table")
    val rscan = table.getScanner(scan).iterator()
    var i = 0
    val delete = new ArrayList[Delete]()

    val map = new HashMap[String, Int]()
    while (rscan.hasNext()) {
      val rs = rscan.next()
      //      delete.add(new Delete(rs.getRow))
      val ca = new String(rs.getValue("f".getBytes, "lb".getBytes))
      if (map.containsKey(ca))
        map.put(ca, map.get(ca) + 1)
      else map.put(ca, 1)
      if (ca == "医疗")
        delete.add(new Delete(rs.getRow))
      i = i + 1
      //      for(cell<-rs.rawCells())
    }
    val kv = map.entrySet().iterator()
    while (kv.hasNext()) {
      val next = kv.next()
      println(next.getKey + ":" + next.getValue)
    }
    println("the number article be deleted:" + delete.size())
    table.delete(delete)
    //    this.putUserMsg()
    //    delete("headlines", conf)
    //    val table=conn.getTable("headlines:tmp_data_table")

    //    val scan = new Scan()
    //    scan.addFamily("result".getBytes)
    //
    //    val filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("textClassfy_split"))
    //    scan.setFilter(filter)
    //    createTable("headlines:tmp_data_table", null)

  }

  def clearColumnWithRegex() {

    val conn = HConnectionManager.createConnection(HBaseConfiguration.create())
    val puts = new ArrayList[Put]()
    val scan = new Scan
    scan.addColumn("p".getBytes, "cnt".getBytes)

    val table = conn.getTable("headlines:item_meta_table")
    val rscan = table.getScanner(scan).iterator()
    var i = 0
    val delete = new ArrayList[Delete]()

    while (rscan.hasNext()) {
      val rs = rscan.next()
      //      delete.add(new Delete(rs.getRow))
      val ca = new String(rs.getValue("p".getBytes, "cnt".getBytes))
      if (ca.equals("此页面是否是列表页或首页？未找到合适正文内容。"))
        delete.add(new Delete(rs.getRow))
      else {
        val put = new Put(rs.getRow)
        put.addColumn("p".getBytes, "cnt".getBytes, HtmlParser.delHTMLTag(ca).getBytes)
        puts.add(put)
        if (puts.size() > 500) {
          println("the number article be updated:" + puts.size())
          table.put(puts)
          puts.clear()
        }
      }
      i = i + 1
      //      for(cell<-rs.rawCells())
    }
    println("the number article be deleted:" + delete.size())
    table.delete(delete)
    //    this.putUserMsg()
    //    delete("headlines", conf)
    //    val table=conn.getTable("headlines:tmp_data_table")

    //    val scan = new Scan()
    //    scan.addFamily("result".getBytes)
    //
    //    val filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("textClassfy_split"))
    //    scan.setFilter(filter)
    //    createTable("headlines:tmp_data_table", null)

  }

  def delete(tableName: String, filter: Result => Boolean) {

    val conn = HConnectionManager.createConnection(HBaseConfiguration.create())

    val scan = new Scan
    scan.addColumn("f".getBytes, "ca".getBytes)
    val table = conn.getTable("headlines:item_meta_table")
    val rscan = table.getScanner(scan).iterator()
    var i = 0
    val delete = new ArrayList[Delete]()

    val map = new HashMap[String, Int]()
    while (rscan.hasNext()) {
      val rs = rscan.next()
      //      delete.add(new Delete(rs.getRow))
      val ca = new String(rs.getValue("f".getBytes, "ca".getBytes))
      if (map.containsKey(ca))
        map.put(ca, map.get(ca) + 1)
      else map.put(ca, 1)
      if (ca != null) {
        i = i + 1
        delete.add(new Delete(rs.getRow))
      }
      //      println(new String(rs.getRow) + "--" + new String(rs.getValue("f".getBytes, "lb".getBytes)))
      //      i = i + 1
      //      for(cell<-rs.rawCells())
    }
    val kv = map.entrySet().iterator()
    while (kv.hasNext()) {
      val next = kv.next()
      println(next.getKey + ":" + next.getValue)
    }
    println(i)
    table.delete(delete)
    //    this.putUserMsg()
    //    delete("headlines", conf)
    //    val table=conn.getTable("headlines:tmp_data_table")

    //    val scan = new Scan()
    //    scan.addFamily("result".getBytes)
    //
    //    val filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("textClassfy_split"))
    //    scan.setFilter(filter)
    //    createTable("headlines:tmp_data_table", null)

  }

  def delete(tableName: String) {

    val admin = new HBaseAdmin(HBaseConfiguration.create())
    println(admin.getTableDescriptor(tableName.getBytes).toString())
    admin.disableTable(tableName.getBytes)
    admin.deleteTable(tableName.getBytes)

    admin.close()

  }

  def delete(nameSpace: String, conf: Configuration): Unit = {

    val admin = new HBaseAdmin(conf)

    val desc = admin.getNamespaceDescriptor(nameSpace)

    val tables = admin.getTableNames()
    tables.foreach { x =>
      if (x.startsWith(nameSpace)) {
        admin.disableTable(x)
        admin.deleteTable(x)
      }
    }

    admin.deleteNamespace(nameSpace)

    admin.close()

  }

  def createTable(name: String, desc: HTableDescriptor): Unit = {

    val conf = HBaseConfiguration.create()
    val admin = new HBaseAdmin(conf)

    val desc = new HTableDescriptor()
    desc.setName(name.getBytes)
    desc.addFamily(new HColumnDescriptor("f".getBytes))
    desc.addFamily(new HColumnDescriptor("kw".getBytes))
    admin.createTable(desc)
    admin.close()

  }

  def putUserAct() {
    val prefix = Array("00000", "0000", "000", "00", "0", "")
    val sub = Array("体育", "科技", "美女", "汽车", "搞笑图片", "国内新闻", "财经", "游戏");
    val itemNum = 1000000
    val userNum = 100000

    val bhvType = Array("click", "buy", "view", "search", "collect", "uncollect")
    val bhvLength = bhvType.length
    val place = "湖南、湖北、广东、广西、河南、河北、山东、山西、江苏、浙江、江西、黑龙江、新疆、云南、贵州、福建、吉林、安徽、四川、西藏、宁夏、辽宁、青海、甘肃、陕西、内蒙古、台湾、北京、上海、天津".split("、")
    val sex = Array(0, 1)

    val conf = HBaseConfiguration.create()
    val conn = HConnectionManager.createConnection(conf)

    val htable1 = conn.getTable("headlines:user_behavior_table")
    val htable2 = conn.getTable("headlines:user_message_table")

    val scan = new Scan
    scan.setCaching(500)
    val rscan = htable2.getScanner(scan)

    val list = new ArrayList[Put]
    var r: Result = null
    while ({ r = rscan.next(); r != null }) {
      val uId = new String(r.getRow)
      for (n <- 0 until Random.nextInt(15)) {
        val time = System.currentTimeMillis() - 24l * Random.nextInt(730) * 60 * 60 * 1000
        val id = uId + "_" + time + "_" + Random.nextInt(itemNum)
        val put = new Put(id.getBytes)
        put.add("bhv".getBytes, "type".getBytes, bhvType(Random.nextInt(bhvLength)).getBytes)
        put.add("bhv".getBytes, "cnt".getBytes, Bytes.toBytes(Random.nextInt(5)))
        put.add("bhv".getBytes, "rating".getBytes, Bytes.toBytes(Random.nextFloat()))
        put.add("bhv".getBytes, "time".getBytes, Bytes.toBytes(time))
        list.add(put)
        if (list.size() > 500) {
          htable1.put(list)
          htable1.flushCommits()
          list.clear()
        }
      }
    }

  }

  def putUserMsg() {
    val prefix = Array("00000", "0000", "000", "00", "0", "")
    val sub = Array("体育", "计算机", "科技", "美女", "汽车", "搞笑图片", "国内新闻", "财经", "游戏");
    val itemNum = 1000000
    val userNum = 100000

    val bhvType = Array("click", "buy", "view", "search", "collect", "uncollect")
    val bhvLength = bhvType.length
    val place = "湖南、湖北、广东、广西、河南、河北、山东、山西、江苏、浙江、江西、黑龙江、新疆、云南、贵州、福建、吉林、安徽、四川、西藏、宁夏、辽宁、青海、甘肃、陕西、内蒙古、台湾、北京、上海、天津".split("、")
    val sex = Array(0, 1)

    val conf = HBaseConfiguration.create()
    val conn = HConnectionManager.createConnection(conf)

    val htable = conn.getTable("headlines:user_message_table")

    val list = new ArrayList[Put]

    for (u <- 0 until userNum) {

      val id = Random.nextInt(10) + "-" + prefix(u.toString().length() - 1) + u
      val put = new Put(id.getBytes)
      put.add("f".getBytes, "place".getBytes, place(Random.nextInt(place.length)).getBytes)
      put.add("g".getBytes, ("dy." + sub(Random.nextInt(sub.length))).getBytes, Bytes.toBytes(1.0f))
      put.add("g".getBytes, ("dy." + sub(Random.nextInt(sub.length))).getBytes, Bytes.toBytes(1.0f))
      put.add("g".getBytes, ("dy." + sub(Random.nextInt(sub.length))).getBytes, Bytes.toBytes(1.0f))
      put.add("f".getBytes, "sex".getBytes, Bytes.toBytes(Random.nextInt(2)))
      list.add(put)
      if (list.size() > 500) {
        htable.put(list)
        htable.flushCommits()
        list.clear()
      }

    }

  }

  def putItemMsg(): Unit = {
    val prefix = Array("00000", "0000", "000", "00", "0", "")
    val sub = Array("狗", "宠物", "猫", "兔子", "拉面", "体育", "科技", "美女", "汽车", "搞笑图片", "国内新闻", "财经", "游戏", "动物世界", "婴儿", "羽毛球", "传销", "学生", "明星八卦");
    val subLength = sub.length / 2
    val catecory = Array("段子", "社会", "汽车", "搞笑", "娱乐", "科技", "体育")
    val itemNum = 1000000
    val userNum = 100000

    val bhvType = Array("click", "buy", "view", "search", "collect", "uncollect")
    val bhvLength = bhvType.length
    val place = "湖南、湖北、广东、广西、河南、河北、山东、山西、江苏、浙江、江西、黑龙江、新疆、云南、贵州、福建、吉林、安徽、四川、西藏、宁夏、辽宁、青海、甘肃、陕西、内蒙古、台湾、北京、上海、天津".split("、")
    val sex = Array(0, 1)

    val conf = HBaseConfiguration.create()
    val conn = HConnectionManager.createConnection(conf)

    val htable = conn.getTable("headlines:item_meta_table")

    val list = new ArrayList[Put]

    for (i <- 0 until itemNum) {

      val id = i.toString()
      val put = new Put(id.getBytes)
      val kw = for (o <- 0 until Random.nextInt(5)) yield sub(Random.nextInt(subLength))
      put.add("f".getBytes, "kw".getBytes, kw.mkString(",").getBytes)
      put.add("f".getBytes, "ca".getBytes, catecory(Random.nextInt(catecory.length)).getBytes)

      list.add(put)
      if (list.size() > 500) {
        htable.put(list)
        htable.flushCommits()
        list.clear()
      }

    }

  }

}

case class Article(tag: String, title: String)