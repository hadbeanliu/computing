package com.recommendengine.compute.api.resource

import java.util.ArrayList

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.util.Bytes

import com.google.gson.Gson
import com.recommendengine.compute.api.model.Item
import com.recommendengine.compute.db.hbase.HbaseServer
import com.recommendengine.compute.exception.ParamsErrorException
import com.recommendengine.compute.metadata.Computing
import com.recommendengine.compute.utils.DateUtil

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.MediaType

object TasteResource {
  //      val conf=Rec

  val scenesMapping = scala.collection.mutable.Map[String, (String, String)]()
  scenesMapping.+=(("reBrowser", ("reBrowser", "看了该商品的人还看过其他商品")),
    ("similarItems", ("similar", "对该商品感兴趣的人也喜欢的商品")),
    ("alsoLikeByBrowser", ("alsoLike", "根据用户浏览猜他喜欢的商品")),
    ("reFocus", ("reFocus", "关注了该商品的人,还关注了其他商品")),
    ("alsoLikeByFocus", ("alsoLikeByBrowser", "根据用户关注猜他喜欢的商品")),
    ("mixRec", ("mixRec", "混合推荐,输入参数为上面几种的权重，输入如下weight=[ss_code1:weight1;ss_code2:weight2]")))
}

@Path(value = "/taste")
class TasteResource extends AbstractResource {

  @GET
  @Path(value = "/report")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def report(@QueryParam("biz_code") biz_code: String,
             @QueryParam("ss_code") ss_code: String,
             @QueryParam("start_time") start_time: String,
             @QueryParam("end_time") end_time: String,
             @QueryParam("req") req: String): String = {

    if (req == null)
      return null

    val input = biz_code + ":" + "feedback"
    val filter1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(ss_code.getBytes))

    val scan = new Scan
    scan.setStartRow(start_time.getBytes)
    scan.setStopRow(end_time.getBytes)
    val table = HbaseServer.getTable(input, null)
    val results = table.getScanner(scan).iterator()

    val recored = new ArrayBuffer[(String, String, String, Int)]

    //format to (time,pv|uv,bhv_type,cnt)
    while (results.hasNext()) {
      val it = results.next()
      val data = for (cell <- it.rawCells()) yield {
        val key = Bytes.toString(CellUtil.cloneRow(cell))
        val q = Bytes.toString(CellUtil.cloneQualifier(cell)).split(Computing.KEY_VALUE_SPLIT_SIGN)
        val f = Bytes.toString(CellUtil.cloneFamily(cell))
        val v = Bytes.toInt(CellUtil.cloneValue(cell))
        (key,f,q(1), v)
      }

      recored.++=:(data)
    }

    val interval = DateUtil.dateCompare(start_time, end_time, DateUtil.DAY_OF_MOUTH)
    val sortType = if (interval <= 90) "byDay" else if (interval <= 180) "byWeek" else "byMonth"
    val reqForm = new Gson().fromJson(req, classOf[Array[String]])
    val response = new ArrayList
    val result = for (key <- reqForm) yield {
      key match {
        
        case "click_uv" => {
          val tag = "click_uv"            // 点击推荐物品的独立访问数
          val data = recored.filter(x => x._3.equals("click") && x._2.equals("uv"))
          (tag, data)

        }
        case "view_uv" => {
          val tag = "view_uv"         // 浏览推荐物品的独立访问次数
          val data = recored.filter(x => x._3.equals("view") && x._2.equals("uv"))
          (tag, data)

        }
        case "consume_uv" => {
          val tag = "consume_uv"        //购买推荐商品的对立次数
          val data = recored.filter(x => x._3.equals("consume") && x._2.equals("uv"))
          (tag, data)

        }
        case "click_pv" => {
          val tag = "click_pv"          //点击推荐物品的次数
          val data = recored.filter(x => x._3.equals("click") && x._2.equals("pv"))
          (tag, data)

        }
        case "view_pv" => {
          val tag = "view_pv"          //浏览推荐物品的次数
          val data = recored.filter(x => x._3.equals("view") && x._2.equals("pv"))
          (tag, data)

        }
        case "consume_pv" => {
          val tag = "consume_pv"         //购买推荐物品的数量
          val data = recored.filter(x => x._3.equals("consume") && x._2.equals("pv"))
          (tag, data)

        }
        case "click_rate" => {       //点击推荐物品的次数/浏览推荐物品次数
          val up_index = recored.filter(x => x._3.equals("click") && x._2.equals("pv"))
          val down_index=recored.filter(x => x._3.equals("view") && x._2.equals("pv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("click_rate",data)
        }
        case "clickuv_rate"         =>{   //点击推荐物品独立次数/浏览推荐物品的独立次数
          val up_index = recored.filter(x => x._3.equals("click") && x._2.equals("uv"))
          val down_index=recored.filter(x => x._3.equals("view") && x._2.equals("uv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("clickuv_rate",data)
        }
        case "consume_rate"         =>{     //购买推荐物品的次数/点击推荐物品的次数
          val up_index = recored.filter(x => x._3.equals("consume") && x._2.equals("pv"))
          val down_index=recored.filter(x => x._3.equals("click") && x._2.equals("pv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("consume_rate",data)
        }
        case "consumeuv_rate"       =>{
          val up_index = recored.filter(x => x._3.equals("consume") && x._2.equals("uv"))
          val down_index=recored.filter(x => x._3.equals("click") && x._2.equals("uv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("consumeuv_rate",data)
        }
        case "total_consume_rate"   =>{
          val up_index = recored.filter(x => x._3.equals("consume") && x._2.equals("pv"))
          val down_index=recored.filter(x => x._3.equals("view") && x._2.equals("pv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("total_consume_rate",data)
        }
        case "total_consumeuv_rate" =>{
          val up_index = recored.filter(x => x._3.equals("consume") && x._2.equals("uv"))
          val down_index=recored.filter(x => x._3.equals("view") && x._2.equals("uv"))
//          val all=view_pv.
          val data=up_index.union(down_index).groupBy(_._1).filter(_._2.length==2).map(f=>(f._1,f._2(0)._4*1.0/f._2(1)._4))
          ("total_consumeuv_rate",data)
        }
      }

    }

    new Gson().toJson(result)
  }

  @GET
  @Path(value = "/doRec")
  @Produces(Array(MediaType.APPLICATION_JSON))
  @throws(classOf[ParamsErrorException])
  def deRec(@QueryParam("biz_code") biz_code: String,
            @QueryParam("ss_code") ss_code: String,
            @QueryParam("user_id") user_id: String,
            @QueryParam("item_id") item_id: String,
            @QueryParam("hm") hm: Int): String = {

    println(biz_code, ss_code, user_id, item_id, hm)
    if (ss_code == null || biz_code == null)
      throw new ParamsErrorException("biz_code=" + biz_code + "&ss_code=" + ss_code)
    
    
    var get=if(user_id!=null){
      HbaseServer.find(biz_code + ":user_item_rec_list", "result", ss_code, user_id)
    }else if(item_id!=null){
      HbaseServer.find(biz_code + ":item_item_rec_list", "result", ss_code, item_id)
    }else{
      HbaseServer.find(biz_code + ":default_rec_list", "result", ss_code, ss_code)
    }

    if (get.isEmpty())
      return "[]"

    var _howMany = hm
    if (_howMany == 0)
      _howMany = 10

    val result = for (item <- Bytes.toString(CellUtil.cloneValue(get.rawCells()(0))).split(Computing.VALUE_KEY_SPLIT_SIGN); if (_howMany > 0)) yield {
      _howMany = _howMany - 1
      val field = item.split(Computing.KEY_VALUE_SPLIT_SIGN)
      Item(field(0), field(1).toFloat, 0)

    }
    
    
    
    new Gson().toJson(result)
  }

  @GET
  @Path(value = "/test/{user_id}/{hm}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  @throws(classOf[ParamsErrorException])
  def test(@QueryParam("user_id") user_id: String,
           @QueryParam("hm") hm: String): String = {

    println(user_id, hm)
    "i am come here"

  }

}