package com.recommendengine.compute.computing


case class PageAct(userId:String,item:String,act:String,cnt:Int,tiem:Long){
  
  def this(userId:String,item:String,act:String)=this(userId,item,act,0,-1)
  
  override
  def toString:String =  s"$userId --> $act -->$item"
  
}
object PageAct extends Serializable {
  
  
  def toPageAct(act:String):PageAct={
    
    val split=act.split("##")
    new PageAct(split(0),split(1),split(2))
    
  }
  
}