package com.recommendengine.compute.lib.similary

object SimilaryCaculate {
  
  
  
  private val lamda=0.1
  
  
  def cosineSimilary(xy:Double,xx:Double,yy:Double)={
    
       xy/(math.sqrt(xx+lamda)*math.sqrt(yy+lamda))
    
  }
  
  def jaccard(xy:Int,x:Int,y:Int)=xy*1.0/(x+y-xy)
  
  def pearsonSimilary(Exy:Double,Ex:Double,Ey:Double,Exx:Double,Eyy:Double)={
        (Exy-Ex*Ey)/math.sqrt((Exx-math.pow(Ex, 2))*(Eyy-math.pow(Ey, 2)))
    
    
  }
  
  
}