package com.recommendengine.compute.lib.classification

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.lib.recommendation.ComputingTool
import org.apache.hadoop.hbase.client.Scan
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.hadoop.hdfs.tools.GetConf
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class NaiveBayesMultiClassifition extends ComputingTool {

  def read = {

    val input = getConf.get(Computing.INPUT_TABLE)

    val sourceTable = getConf.get(Computing.DATA_SOURCE);

    val scan = new Scan()
    scan.addFamily("pc".getBytes)

    val dataSource = HbaseServer.get(sourceTable, scan, sc, result => {

      val cells = result.rawCells()
      var content = ""
      var category = ""
      var tags = ""
      for (cell <- cells) {

        val q = Bytes.toString(CellUtil.cloneQualifier(cell))
        val v = Bytes.toString(CellUtil.cloneValue(cell))
        if (q.equals("content")) {
          content = v.replaceAll("(?is)<script.*?>.*?</script>", "").replaceAll("(<p>|</p>)", "").replaceAll("(?is)<div.*?>.*?</div>", "").replaceAll("(?is)<!--.*?-->", "").replaceAll("<.*?>", "").replaceAll("&nbsp", "")
        } else if (q.equals("category"))
          category = v
        else if (q.equals("tags"))
          tags = v
      }
      (category,content)
    })
    dataSource
  }

  def run = {
    
    val minDocFreq=if(this.args.get("minDocFreq")!=null) this.args.get("minDocFreq").asInstanceOf[String].toInt else 2
    val lamba=1.0
    val ssCode=getConf.get(Computing.COMPUTING_BITCH_ID)
    val bizCode=getConf.get(Computing.COMPUTING_ID)
    
    val hashtf=new HashingTF
    val data=read
    val traingdata=data.map(f=>((f._1,hashtf.indexOf(f._1)),hashtf.transform(f._2)))

    val idf=new IDF(minDocFreq).fit(traingdata.values)
  
    val input=traingdata.map(f=>(LabeledPoint(f._1._2,idf.transform(f._2))))
    
    val model=NaiveBayes.train(input, lamba, "multinomial")
    
    val savepath="/computing/model/naiveBayes/"+bizCode+"-"+ssCode
    
    val p=new Path(savepath)
    
    val fs=FileSystem.get(getConf)
    
    if(fs.exists(p)){
       fs.delete(p,true)
    }
    
    model.save(sc, savepath)
    
    null
        
  }

  def write[item: ClassTag](data: RDD[item], op: item => (ImmutableBytesWritable, Put)) = {

    null
  }
}