package com.recommendengine.compute.lib.classification

import java.util.HashMap

import com.google.gson.Gson
import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.lib.recommendation.ComputingTool
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.TextSplit
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag


class StringSplit extends ComputingTool{
  private val log = LoggerFactory.getLogger(classOf[WordSplit])
  private val STOP_WORD_PATH = "/computing/mining/data/stopWord.txt"

  private val DEFAULT_OUTPUT = "tmp_data_table"
  def read = {

    val ssCode = getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode = getConf.get(Computing.COMPUTING_ID)
//    HbaseServer.clearTable(bizCode + ":" + DEFAULT_OUTPUT)
    val source = this.args.get(Computing.DATA_SOURCE).asInstanceOf[String]
    val input = bizCode + ":" + this.args.get(Computing.INPUT_TABLE).asInstanceOf[String]
    val tagCol = if (this.args.get("category.col") != null) this.args.get("category.col").asInstanceOf[String] else "f:t"
    val textCol = if (this.args.get("content.col") != null) this.args.get("content.col").asInstanceOf[String] else "p:t"

    val default = if (source == null) input else source
    val scan = new Scan() 
    scan.addColumn(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes)
    scan.addColumn(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes);
    val dataSource =if(this.args.get("dataSource").asInstanceOf[String]== "hdfs"){
      val splitRegex=this.args.get("dataSource.splitRegex").asInstanceOf[String]
      val filter= if(this.args.get("label.filter") !=null) this.args.get("label.filter").asInstanceOf[String] else null

      sc.textFile(this.args.get("dataSource.path").asInstanceOf[String]).map(line=> (line.split(splitRegex)(0),line.split(splitRegex)(1)))
                      .filter(x=>filter.indexOf(x._1)== -1)

    } else HbaseServer.get(default, scan, sc, result => {

      
      val category = Bytes.toString(result.getValue(tagCol.split(":")(0).getBytes, tagCol.split(":")(1).getBytes))
      val content = Bytes.toString(result.getValue(textCol.split(":")(0).getBytes, textCol.split(":")(1).getBytes))
      if(content==null||category==null)
        println(Bytes.toString(result.getRow))
      (category, content)
    })
    dataSource
  }

  def run = {

    val result = new HashMap[String, Any]
    LOG.info("开始任务:"+this.getClass)
    LOG.info("任务参数::"+new Gson().toJson(this.args))
    LOG.info("开始从hbase读取数据....")

    val data=read

    val minContentSize=if(this.args.get("minContentSize")==null) 20 else this.args.get("minContentSize").asInstanceOf[String].toInt
    val ss=SparkSession.builder().getOrCreate()
    
//    val data = ss.sparkContext.wholeTextFiles("file:///home/hadoop/result/train").map(f=>(f._1.substring(f._1.lastIndexOf("/")+2),f._2))
    
    import ss.implicits._

    LOG.info("读取数据库 hbase：[[[ "+data.count()+" ]]] 条将被进行分词")
    val df=data.map(ar=>(ar._1,TextSplit.process(ar._2))).toDF("label","setence")
        
    df.show()
    LOG.info("成功进行分词")

    val stopWord=ss.sparkContext.textFile(STOP_WORD_PATH).collect()


    val stopWordRemover=new StopWordsRemover
    stopWordRemover.setStopWords(stopWord)
    LOG.info("加载停用词库:"+STOP_WORD_PATH)

    stopWordRemover.setInputCol("setence").setOutputCol("words")
    val df2=stopWordRemover.transform(df)
    println(df2.count())
    LOG.info("停用词处理完毕，剩余 "+df2.count()+" 条将被导入下一个任务")

    //    val df3=df2.select("label", "words").rdd.flatMap { x => {
//      val r=x.getAs[Seq[_]]("words")
//      val label =x.getAs[String]("label")
//        for(word<-r)yield ((label,word),1)  
//    } }
//    
//     
    
//    df3.reduceByKey(_ + _).map(f=>(f._1._1,(f._1._2,f._2))).groupByKey().map(f=>{
//      
//      val file=new File("/home/hadoop/result/words-with-split/"+f._1)
//      val write=new PrintWriter(file)
//      f._2.foreach(x=>write.println(x._1+" "+x._2))
//      write.flush()
//      write.close()
//      f._1
//    }).count
    
//    df3.saveAsTextFile("file:///home/hadoop/result/word-with-split")
    
//    df2.createOrReplaceTempView("splitTxt")
    LOG.info("保存数据到 table.splitTxt ")

    df2.select("label", "words").createOrReplaceTempView("splitTxt")
    result
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

    val output = this.args.get(Computing.OUTPUT_TABLE)
    val bizCode = getConf.get(Computing.COMPUTING_ID)

    val tableName = bizCode + ":" + DEFAULT_OUTPUT

    println(tableName, output)
    val jobConf = new JobConf(getConf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    data.map { op }.saveAsHadoopDataset(jobConf)

  }
  
  
}