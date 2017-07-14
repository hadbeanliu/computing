package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.recommendengine.compute.lib.recommendation.BaseRatingRuleRecommend
import org.apache.hadoop.conf.Configuration
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.lib.recommendation.UserGraphAnalysis
import com.recommendengine.compute.lib.classification.WordSplit
import org.apache.spark.sql.SparkSession
import com.recommendengine.compute.lib.classification.StringSplit
import com.recommendengine.compute.lib.classification.FeatureSelect
import com.recommendengine.compute.lib.classification.ClassifyModelBuilder
import com.recommendengine.compute.conf.ComputingConfiguration
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.IndexToString
import com.recommendengine.compute.utils.TextSplit
import org.apache.spark.ml.feature.IDFModel
import scala.collection.mutable.Seq
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.SparseVector

object SubmitJobTest {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("test")

    val ss = SparkSession.builder().appName("simple").master("local[*]").config(sconf).getOrCreate()
    val conf = ComputingConfiguration.create()
    import org.apache.spark.sql.functions.udf
    val text="日本松下公司推出了一个名为nanoe x的黑科技——可以对衣物进行除臭的衣架"
        val splitWords=TextSplit.process(text)
    val wordSize=splitWords.size
    val data = ss.createDataFrame(Seq((0,splitWords))).toDF("id", "words")
    
    val tf = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").transform(data)
        
    
    
    val func = udf {
      vec:Vector=>
         val v=vec.toSparse
         val size=v.values.count { x => x>0 }
         val values=v.values.map { x => x/size }
         println(size)
         new SparseVector(v.size,v.indices,values)
    }
    val withTf=tf.withColumn("rowFeatures",func(tf("rowFeatures")))

    println(tf.select("rowFeatures").take(1).apply(0).getAs[Vector](0).toSparse.values.sum)
    
    println(withTf.select("rowFeatures").take(1).apply(0).getAs[Vector](0).toSparse.values.sum)
    
 }

  def naiveTest() {

    val sconf = new SparkConf().setAppName("test")

    val ss = SparkSession.builder().appName("simple").master("local[*]").config(sconf).getOrCreate()
    val conf = ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")

    val sc = ss.sparkContext

    val text = "虎扑篮球 5 月 17 日讯 在今天的乐透抽签大会上，76 人抽中了探花签，会后，76 人代表乔尔 - " +
      "恩比德接受了采访。谈到探花签，恩比德表示：'我喜欢约什 - 杰克逊和杰森 - 塔图姆，我期待他们之一会被我们的探花签选中。'" +
      "谈到球队的未来，恩比德表示：' 我们会在合适的时间准备好，当我们开始崛起，骑士和勒布朗（詹姆斯）将会开始衰落。" +
      "当我说我们会准备好赢球，同时骑士会衰落时，我并不是指未来五年，下个赛季，我认为我们就会开始准备好赢球了。' 恩比德说道。" +
      "此外，恩比德也表示下赛季他会努力出战每一场比赛。" +
      "2016-17 赛季，恩比德因伤仅出战了 31 场比赛，场均出战 25.4 分钟，能够得到 20.2 分 7.8 篮板 2.1 助攻 2.45 盖帽";

    val idfModel = IDFModel.load(conf.get("default.model.path") + "/" + "headlines_user-analys" + "/" + IDFModel.getClass.getSimpleName)

    val data = ss.createDataFrame(Seq((0, TextSplit.process(text)))).toDF("id", "words")

    val tf = new HashingTF().setInputCol("words").setOutputCol("rowFeatures")

    val tfidf = idfModel.transform(tf.transform(data))
    tfidf.show()

    val stringModel = StringIndexerModel.load(conf.get("default.model.path") + "/" + "headlines_user-analys" + "/" + StringIndexerModel.getClass.getSimpleName)

    val naiveBayesModel = NaiveBayesModel.load(conf.get("default.model.path") + "/" + "headlines_user-analys" + "/" + NaiveBayesModel.getClass.getSimpleName)

    naiveBayesModel.transform(tfidf).select("probability").collect()(0).getAs[DenseVector](0).foreachActive((i: Int, v: Double) => println(i, v))

    //    val stringModel=StringIndexerModel.load(conf.get("default.model.path")+"/"+"headlines_user-analys"+"/"+StringIndexerModel.getClass.getSimpleName)
    //    stringModel.tr
    //    stringModel.
    //    stringModel.transform(dataset)
    //    val indextoString=new IndexToString().setInputCol("intLabel").setOutputCol("ogLabel")

    //    indextoString.transform(stringModel)

  }

  private def baseRatingRule() {

    val sconf = new SparkConf().setMaster("local[*]").setAppName("test")

    val sc = new SparkContext(sconf)
    val conf = ComputingConfiguration.create()
    conf.set(Computing.COMPUTING_ID, "headlines")
    conf.set(Computing.COMPUTING_BITCH_ID, "user-analys")

  }
}