package com.recommendengine.compute.lib.similary

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.SparseVector

class Similary {
  
  def calculate(v1:Vector,v2:Vector):Double={
            
      (v1,v2) match{
        case (SparseVector(size1,indics1,values1),SparseVector(size2,indics2,values2))=>{
          
            var xy: Double = 0
            var xx: Double = 0
            var yy: Double = 0

            val length1 = indics1.length
            val length2 = indics2.length
            var preIndex1 = 0
            var preIndex2 = 0
            var flag = false
            while (preIndex1 < length1 && preIndex2 < length2) {
              if (indics1(preIndex1) == indics2(preIndex2)) {
                xy += values1(preIndex1) * values2(preIndex2)
                xx += values1(preIndex1) * values1(preIndex1)
                yy += values2(preIndex2) * values2(preIndex2)
                preIndex1+=1
                preIndex2+=1
              } else if (indics1(preIndex1) > indics2(preIndex2)) {
                xx += values1(preIndex1) * values1(preIndex1)
                yy += values2(preIndex2) * values2(preIndex2)
                preIndex2 += 1
              } else {
                xx += values1(preIndex1) * values1(preIndex1)
                yy += values2(preIndex2) * values2(preIndex2)
                preIndex1 += 1
              }

            }
          
            xy / Math.sqrt(xx * yy)
  
          
        }
        case other =>0.0
      }
                 
  }
  
}