package com.recommendengine.compute.api.resource

import javax.ws.rs.Path
import javax.ws.rs.POST
import javax.ws.rs.GET
import com.recommendengine.compute.metadata.Computing
import javax.ws.rs.QueryParam
import org.apache.hadoop.hbase.client.Get
import com.recommendengine.compute.db.hbase.HbaseServer
import org.apache.hadoop.hbase.client.HConnectionManager
import com.recommendengine.compute.api.RecServer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.PrefixFilter
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import org.apache.hadoop.hbase.client.Scan
import com.google.gson.Gson
import org.apache.hadoop.hbase.client.Put

@Path("/db")
class DBResource extends AbstractResource {

  @POST
  @Path("/update")
  def update(value:String,@QueryParam("tableName") tableName: String,@QueryParam("rowKey")rowKey:String, @QueryParam("family") family: String, @QueryParam("Qualifier") q: String): Any = {

    require(rowKey!=null&&tableName != null&&tableName!=null&&q!=null, s"update message :tableName")

    

    val hconn = HConnectionManager.createConnection(MiningResource.conf)
    val table = hconn.getTable(tableName)
   
    try{
      
      val put=new Put(rowKey.getBytes)
      put.addImmutable(family.getBytes, Bytes.toBytes(q), Bytes.toBytes(value)) 
      table.put(put)
      
      
    } catch {
      case e: Exception => e.printStackTrace()
      return "failed:"+e.getMessage

    } finally {
      table.flushCommits()
      table.close()
      hconn.close()
    }

    "success"
  
  }

  @GET
  @Path("/select")
  @Produces(Array(MediaType.APPLICATION_ATOM_XML))
  def select(@QueryParam("tableName") tableName: String, @QueryParam("family") family: String, @QueryParam("qualifier") q: String): String = {

    val hconn = HConnectionManager.createConnection(MiningResource.conf)
    val table = hconn.getTable(tableName)
    try {
      val scan = new Scan
      if (q != null)
        scan.addColumn(family.getBytes, q.getBytes)
      else scan.addFamily(family.getBytes)

      val result = table.getScanner(scan).next(100)
      val arrs=result.map { r =>
        for(cell<-r.rawCells())yield{
           (Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)))
        }
        }
      

      return new Gson().toJson((arrs.length,arrs))

    } catch {
      case e: Exception => e.printStackTrace()

    } finally {
      table.close()
      hconn.close()
    }

    null
  }

  @GET
  @Path("/get")
  def get(@QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String, version: String): String = {

    null
  }

  @GET
  @Path("/log")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getLog(@QueryParam("biz_code") bizCode: String, @QueryParam("ss_code") ssCode: String): String = {
    println("conhe heee")
    require(bizCode != null && ssCode != null, s"[biz_code=$bizCode],[ss_code=$ssCode]")
    val hconn = HConnectionManager.createConnection(HBaseConfiguration.create())
    val htable = hconn.getTable(bizCode + ":history")
    var log: String = ""

    try {
      val get = new Get(ssCode.getBytes)
      get.addFamily("log".getBytes)

      get.setFilter(new PrefixFilter(ssCode.getBytes))
      val result = htable.get(get)
      var maxTime: Long = Long.MinValue

      println(result.size())
      for (cell <- result.rawCells()) {
        val q = Bytes.toLong(CellUtil.cloneQualifier(cell))
        println(q)
        if (q > maxTime) {
          maxTime = q
          log = Bytes.toString(CellUtil.cloneValue(cell))
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      htable.close()
      hconn.close()
    }
    log
  }

}

