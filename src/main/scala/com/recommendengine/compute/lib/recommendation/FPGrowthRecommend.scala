package com.recommendengine.compute.lib.recommendation

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.metadata.Computing

class FPGrowthRecommend extends ComputingTool with Readable with Writable{

  private val MIN_SUPPORT = "minSupport"

  private val MIN_CONFIDENCE = "minConfidence"

  def read = {

    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val input = this.args.get(Computing.INPUT_TABLE)

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val table = bizCode + ":" + input

    val scan = HbaseServer.buildScan(Array(("relate", Array(ssCode))), null, null, null)
    var data = HbaseServer.flatGet(table, scan, sc,  result =>
      {

        val cell = result.rawCells()(0)
        val v = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.VALUE_KEY_SPLIT_SIGN)
        for (freq <- v) yield freq.split(Computing.KEY_KEY_SPLIT_SIGN)
      }

    )
    data

  }

  def run() = {

    val dataModel = read 

    val minSupport = getConf.getDouble(MIN_SUPPORT, 0.2)
    val minConfidence = getConf.getDouble(MIN_CONFIDENCE, 0.8)

    val bizCode = getConf.get(Computing.COMPUTING_ID)
    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)

    val model = new org.apache.spark.mllib.fpm.FPGrowth().setMinSupport(minSupport).run(dataModel)

    val rules=model.generateAssociationRules(minConfidence)
    
    rules.map { x => x }
    
//    write[AssociationRules.Rule[String]]
    null
  }
  
 def write[item:ClassTag] (data:RDD[item],op:item=>(ImmutableBytesWritable,Put))={
   
 }

}