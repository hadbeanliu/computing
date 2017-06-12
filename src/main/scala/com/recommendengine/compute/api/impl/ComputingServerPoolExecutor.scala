package com.recommendengine.compute.api.impl

import java.util.Collection
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import org.apache.commons.collections.CollectionUtils

import com.google.common.collect.Queues

class ComputingServerPoolExecutor(corePoolSize:Int,maxPoolSize:Int,keepAliveTime:Long,unit:TimeUnit,queue:BlockingQueue[Runnable]) extends
                ThreadPoolExecutor(corePoolSize:Int,maxPoolSize:Int,keepAliveTime:Long,unit:TimeUnit,queue:BlockingQueue[Runnable]){
  
  
      private val runningTask:Queue[TaskWorker]=Queues.newArrayBlockingQueue(maxPoolSize)
      private val historyTask:Queue[TaskWorker]=Queues.newArrayBlockingQueue(maxPoolSize)
      
     override def beforeExecute(t:Thread, r:Runnable){
        
        super.beforeExecute(t, r)
        
        synchronized(runningTask).offer(r.asInstanceOf[TaskWorker])
        
        
        
      }
      
      
      override def afterExecute(r:Runnable, t:Throwable){
        super.afterExecute(r, t)
        synchronized(runningTask).remove(r.asInstanceOf[TaskWorker])
        
      }
      
      
      def getAllTasks():java.util.List[TaskInfo]=CollectionUtils.union(getRunningTasks() , getHistoryTasks()).asInstanceOf[java.util.List[TaskInfo]]
        
      
      def getRunningTasks()=getTaskInfo(this.runningTask)
      
      
      def getHistoryTasks()=getTaskInfo(this.historyTask)
      
      
      
      private def  getTaskInfo(tasks:Collection[TaskWorker]): java.util.List[TaskInfo]={
        
        val it=tasks.iterator();
        val list=new LinkedList[TaskInfo]
        while(it.hasNext()){
          list.add(it.next().getInfo)
        }
        list
      }
  
}