package com.recommendengine.compute.db.hbase

import scala.reflect.ClassTag

import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin

import com.recommendengine.compute.conf.ComputingConfiguration
import org.restlet.resource.ClientResource
import com.recommendengine.compute.utils.TextSplit
import com.chenlb.mmseg4j.SimpleSeg
import com.chenlb.mmseg4j.Dictionary
import com.chenlb.mmseg4j.example.Complex
import com.chenlb.mmseg4j.ComplexSeg
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.ivy.core.module.descriptor.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.mutable.ArrayBuilder
import java.util.StringTokenizer

object Test {
  
  val arr=Array("1","2","3","4")
  
  def main(args:Array[String]){
    val overwrite=false
    val path="/computing/mining/data/test.txt"
    val seedDir = new Path(path)
    val str="i am ok"
    val fs = FileSystem.get(HBaseConfiguration.create())

    try {
      if (!overwrite) {
        fs.deleteOnExit(seedDir)
      }
      val os = fs.create(seedDir)
      os.write(str.getBytes)
      os.flush()
      os.close()
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {

      fs.close()

    }

    true
  }
  
  def nonNagetiveMod(x:Int,mod:Int):Int={
    
    val rowMod=x%mod
    println(rowMod)
    rowMod + (if(rowMod<0) mod else 0)

  }
  
  
  def get[A:ClassTag](p: String=>A):Array[A]={
    
    val result=for(str<-arr)yield p(str)
    result
  }
  
  
}