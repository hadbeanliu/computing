package com.test

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.SQLUserDefinedType
import org.restlet.resource.ClientResource
import com.recommendengine.compute.api.model.WebPage

object HttpTest {
  
  
  def main(args0:Array[String]):Unit={
        
    val page=new WebPage(123123)
    page.setTitle("山西省政协原副主席令政策受贿案一审开庭")
    page.setContent("2016年10月18日，江苏省常州市中级人民法院公开开庭审理了山西省政协原副主席令政策受贿一案。常州市人民检察院派员出庭支持公诉，被告人令政策及其辩护人到庭参加诉讼。常 州市人民检察院指控：被告人令政策利用担任山西省发展计划委员会副主任、常务副主任、山西省发展和改革委员会主任、山西省政协副主席等职务上的便利，以及 本人职权、地位形成的便利条件，通过其他国家工作人员职务上的行为，为洪洞华清煤焦化学有限公司、杜善学等单位和个人在项目审批、获取用地、企业经营、职 务晋升、岗位调整等事项上提供帮助，直接或通过其子令狐帅非法收受上述单位和人员给予的财物，共计折合人民币1607.2374万元。提请以受贿罪追究令 政策的刑事责任。庭审中，检察机关出示了相关证据，令政策及其辩护人进行了质证，控辩双方充分发表了意见，令政策进行了最后陈述，当庭表示认罪悔罪。全国、省、市人大代表、政协委员、新闻记者及各界群众六十余人旁听了庭审")
    val feature=new java.util.HashMap[String,Any]
    feature.put("tags", " 令政策 官员贪腐 受贿")
    feature.put("category", "国内新闻")
    page.setFeature(feature)
    val client=new ClientResource("http://192.168.16.111:9999/mining/classify?biz_code=topNews&ss_code=autoClassify&model=NaiveBayes")
         
         val res=client.post(page)
         println(res.getText)
//         client.set
         
       }
  
}

case class People(name:String,age:Int)