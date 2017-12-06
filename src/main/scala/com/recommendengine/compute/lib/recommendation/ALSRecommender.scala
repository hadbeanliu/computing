package com.recommendengine.compute.lib.recommendation

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.slf4j.LoggerFactory

import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.StringUtils

class ALSRecommender extends ComputingTool with Readable with Writable {

  private type inputType = RDD[Rating]
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private val result=new java.util.HashMap[String,Any]()

  //  private type outPutType= RDD[]
  

  private var _ALSModel: MatrixFactorizationModel = null

  def getArgs = this.args

  def read= {
    val conf = getConf()

    val bizCode = conf.get("bizCode")
    val ssCode = conf.get("ssCode")
    val input = this.args.get(Computing.INPUT_TABLE)
  
    val scan = HbaseServer.buildScan(Array(("behavior", Array(ssCode))), null, null, null)
    val table = bizCode + ":" + input

    HbaseServer.flatGet(table, scan, sc, 
      result =>
        {
          val user = Bytes.toString(result.getRow)
          val intUser = StringUtils.getNonNegatived(user.hashCode, Int.MaxValue)
          val cell = result.rawCells()(0)

          val itemWithValue = Bytes.toString(CellUtil.cloneValue(cell)).split(Computing.KEY_VALUE_SPLIT_SIGN)
          for (kv <- itemWithValue) yield {

            val item = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(0)
            val value = kv.split(Computing.KEY_VALUE_SPLIT_SIGN)(1)
            val intItem = StringUtils.getNonNegatived(item.hashCode(), Int.MaxValue)

            (user, Rating(intUser, intItem, value.toDouble), item)
          }

        }

    )
 
  }

  def run()= {
    
    result.clear()
    LOG.info("ALS start running at", sdf.format(System.currentTimeMillis()))

    LOG.info("ALS start read data from ", this.args.get(Computing.INPUT_TABLE))
    

    
    val _dataModel: RDD[(String, (Rating), String)] = read 

    val dataModel = _dataModel.map(_._2)

    val _implicitPrefs = getArgs.get("implicitPrefs").asInstanceOf[Boolean]
    val _numIterators = getArgs.get("numIterators").asInstanceOf[Int]
    val _rank = getArgs.get("rank").asInstanceOf[Int]
    val _lambda = getArgs.get("lambda").asInstanceOf[Double]
    var _topK = getArgs.get("topK").asInstanceOf[Int]

    if (_topK <= 0)
      _topK = 1

    _ALSModel = new ALS().setImplicitPrefs(_implicitPrefs)
      .setIterations(_numIterators)
      .setLambda(_lambda)
      .setRank(_rank)
      .run(dataModel)

    val intIdWithUser = _dataModel.map(f => (f._2.user, f._1)).distinct()
    val intIdWithItem = _dataModel.map(f => (f._2.product, f._3)).distinct()

    val stringUserResult = _ALSModel.recommendProductsForUsers(_topK).join(intIdWithUser).flatMap(f => {
      f._2._1.map(rating => (rating.product, f._2._2, rating.rating))
    })

    val finalResult = stringUserResult.groupBy(_._1).join(intIdWithItem).flatMap(f => {

      f._2._1.map(group => (group._2, f._2._2, group._3))

    }).groupBy(_._1)

    val ssCode=getConf.get(Computing.COMPUTING_BITCH_ID)
    write[(String, Iterable[(String, String, Double)])](finalResult, item => {
      val put = new Put(item._1.getBytes)
      val buff=new StringBuffer
      item._2.foreach(f => {
          buff.append(f._2).append(Computing.KEY_VALUE_SPLIT_SIGN).append(f._3).append(Computing.VALUE_KEY_SPLIT_SIGN)
      })
      put.add("result".getBytes, ssCode.getBytes , buff.toString().getBytes)
      (new ImmutableBytesWritable(),put)
    })
    
    result
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

  def recommendForUsers(num: Int): RDD[(Int, Array[Rating])] = {

    _ALSModel.recommendProductsForUsers(num)
  }

  def computeRMSE(ratings: RDD[Rating]): Double = {

    val predictsAndPrefs = this._ALSModel.predict {
      ratings.map { x => (x.user, x.product) }
    }.map { x => ((x.user, x.product), x.rating) }.join(ratings.map { x => ((x.user, x.product), x.rating) })

    Math.sqrt(predictsAndPrefs.values.map {
      ratings => (ratings._1 - ratings._2) * (ratings._1 - ratings._2)

    }.mean())

  }

  def recommenderUsers(item: Int, num: Int): Array[Rating] = {

    if (_ALSModel == null)
      throw new RuntimeException("模型未初始化，请先初始化模型")

    _ALSModel.recommendUsers(item, num)
  }

  def recommenderItems(user: Int, num: Int): Array[Rating] = {

    if (_ALSModel == null)
      throw new RuntimeException("模型未初始化，请先初始化模型")

    _ALSModel.recommendProducts(user, num)

  }

  //  def loadFromHbase()

  def saveToHBase(conf: Configuration, tableName: String, p: (Int, Array[Rating]) => Put, family: String, q: String): Unit = {

    if (_ALSModel != null) {
      
      val jobConf = new JobConf(conf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

      val recommendItems = _ALSModel.recommendProductsForUsers(100).map {
        case (row, items) => {
          val put: Put = new Put(Bytes.toBytes(row))
          val sb = new StringBuffer
          items.foreach { rating => sb.append(rating.product).append(":").append(rating.rating).append(",") }
          put.add(family.getBytes, q.getBytes, sb.toString().getBytes)
          (new ImmutableBytesWritable, put)
        }
      }
      recommendItems.saveAsHadoopDataset(jobConf)

    }

  }

}