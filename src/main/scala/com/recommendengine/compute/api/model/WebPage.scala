package com.recommendengine.compute.api.model

class WebPage(private var id:Long,private var content:String,
    private var title:String,private var feature:java.util.Map[String,Any])  {
    
    def this(id:Long)=this(id,null,null,null)
    
    def setId(id:Long)=this.id=id
    
    def getId=this.id
    
    def setTitle(title:String)=this.title=title
    
    def getTitle=this.title
    
    def setContent(content:String)=this.content=content
    
    def getContent=this.content
    
    def setFeature(feature:java.util.Map[String,Any])=this.feature=feature
    
    def getFeature=this.feature
}


