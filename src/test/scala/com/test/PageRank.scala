package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy
import scala.collection.mutable.Map

object PageRank {
  
  
  
  def main(args:Array[String]){
    
    
    
    val conf=new SparkConf().setAppName("PageRank")
    
    val sc=new SparkContext(conf)

    val unPartitionGraph=GraphLoader.edgeListFile(sc,
        "/home/hadoop/Documents/soc-LiveJournal1.txt", 
        false, 4, StorageLevel.MEMORY_ONLY , StorageLevel.MEMORY_ONLY).cache()
        
     
    val graph=unPartitionGraph.partitionBy(PartitionStrategy.EdgePartition1D)
    
     println("the size of vertice:"+graph.vertices.count())
     println("the size of edge:"+graph.edges.count())
     
     val pr=org.apache.spark.graphx.lib.PageRank.run(graph, 20 ,0.15).vertices.cache()
     println("Total rank is "+pr.map(_._2).reduce(_+_))
     pr.top(2).foreach{case (id,rank)=>println(id,rank)}
     
  }
  
  
}