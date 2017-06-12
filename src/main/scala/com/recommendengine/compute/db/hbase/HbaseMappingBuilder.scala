package com.recommendengine.compute.db.hbase

import java.util.HashMap
import java.util.Map

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.regionserver.BloomType
import java.lang.Boolean

object HbaseMappingBuilder {
  
  private val tableToFamilys=new HashMap[String,HashMap[String,HColumnDescriptor]]()
  
  
  
  def build( tableName:String,  familyName:String,
         compression:String,  blockCache:String,  blockSize:String,
         bloomFilter:String , maxVersions:String,  timeToLive:String, 
         inMemory:String):HColumnDescriptor={
         
          val familys=HbaseMappingBuilder.getOrCreateFamilys(tableName)
           val columnDescriptor=HbaseMappingBuilder.getOrCreateCol(familyName, familys)
           
           if(compression != null)
              columnDescriptor.setCompressionType(Algorithm.valueOf(compression));
           if(blockCache != null)
              columnDescriptor.setBlockCacheEnabled(Boolean.parseBoolean(blockCache));
           if(blockSize != null)
              columnDescriptor.setBlocksize(Integer.parseInt(blockSize));
           if(bloomFilter != null)
              columnDescriptor.setBloomFilterType(BloomType.valueOf(bloomFilter));
           if(maxVersions != null)
              columnDescriptor.setMaxVersions(Integer.parseInt(maxVersions));
           if(timeToLive != null)
              columnDescriptor.setTimeToLive(Integer.parseInt(timeToLive));
           if(inMemory != null)
              columnDescriptor.setInMemory(Boolean.parseBoolean(inMemory));  
           columnDescriptor
         
      } 
  
  
  private def getOrCreateFamilys(name:String):Map[String,HColumnDescriptor]=if(tableToFamilys.containsKey(name)) return tableToFamilys.get(name)
                    else {
                      val familys=new HashMap[String,HColumnDescriptor]()
                      tableToFamilys.put(name, familys)
                      return familys
                      }
     
    
  private def getOrCreateCol(name:String,hCols:Map[String,HColumnDescriptor]):HColumnDescriptor=if(hCols.containsKey(name)) hCols.get(name) else{ 
              val hCol=new HColumnDescriptor(name)      
              hCols.put(name, hCol)
              hCol            
                }
  
  
}