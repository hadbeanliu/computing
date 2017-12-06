package com.recommendengine.compute.db.hbase

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.ConnectionFactory

object HbaseServer {

  val defaultconf = HBaseConfiguration.create()
  
  def clearTable(table:String){
    
    val admin=new HBaseAdmin(defaultconf)
    val desc=admin.getTableDescriptor(table.getBytes)
    admin.disableTable(table.getBytes)
    admin.deleteTable(table.getBytes)
    admin.createTable(desc)
    admin.close()
  }

  def flatGet[A: ClassTag](table: String, scan: Scan, sc: SparkContext, p: Result => Array[A]): RDD[A] = {

    val conf = HBaseConfiguration.create()
    
    conf.set(TableInputFormat.INPUT_TABLE, table)

    conf.set(TableInputFormat.SCAN, converScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[Result])
    hbaseRDD.values.flatMap { p(_) }
  }

  def find(table: String, family: String, q: String, raw: String): Result = {

    val htable = getTable(table, null)
    val get = new Get(raw.getBytes)
    if (q == null) get.addFamily(family.getBytes)
    else get.addColumn(family.getBytes, q.getBytes)
    htable.get(get)
    
  }
  

  def get[A: ClassTag](table: String, scan: Scan, sc: SparkContext, p: Result => A): RDD[A] = {

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, table)

    conf.set(TableInputFormat.SCAN, converScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[Result])
    hbaseRDD.values.map { p(_) }
  }

  def buildScan(args: Array[(String, Array[String])], startKey: String, endKey: String,filter:FilterList): Scan = {
    val scan = new Scan()
    if (startKey != null)
      scan.setStartRow(startKey.getBytes)
    if (endKey != null)
      scan.setStopRow(endKey.getBytes)

    for ((f, cols) <- args) {
      if (cols != null && cols.length > 0)
        for (q <- cols)
          scan.addColumn(f.getBytes, q.getBytes)
      else if (f != null)
        scan.addFamily(f.getBytes)

    }
    if(filter!=null)
      scan.setFilter(filter)
    scan
  }

  private def converScanToString(scan: Scan): String = {

    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }

  def save(put: Put, conf: Configuration, tableName: String): Boolean = {
    var table:HTableInterface = null
    try {
      table=getTable(tableName, conf)
      table.put(put)
      table.flushCommits()
    } catch {

      case e: Exception => 
        e.printStackTrace()
        return false

    } finally {
      
      table.close()
    }

    true
  }
  
    def saveList(put: java.util.List[Put], conf: Configuration, tableName: String): Boolean = {
    var table:HTableInterface = null
    try {
      table=getTable(tableName, conf)
      table.put(put)

    } catch {

      case e: Exception => return false

    } finally {
      table.close()
    }

    true
  }


  def getTable(tableName: String, conf: Configuration): HTableInterface = new HTable(conf,tableName)

}