//package com.recommendengine.compute.utils
//
//import org.apache.commons.httpclient.HttpClient
//import java.util.Map
//import org.apache.commons.httpclient.HttpMethod
//import org.apache.commons.httpclient.methods.GetMethod
//import org.apache.commons.httpclient.methods.PostMethod
//import org.apache.commons.httpclient.NameValuePair
//import scala.collection.mutable.ArrayBuffer
//
//object HttpUtil {
//  
//  
//  class MethodSignal extends Enumeration{
//    type MethodSignal=Value
//    
//    val POST,GET,DELETE,PUT=Value
//    
//  }
//  
//  
//  def doSend(uri:String,queue:Map[String, String] ,method:String):String={
//		var msg="";
//		val client=new HttpClient();
//		
//		var httpmethod:HttpMethod =null;
////		MethodSignal met = null;
//		
//		if(method.toUpperCase().equals("GET")){
//		  httpmethod=new GetMethod(uri)
//		}else if(method.toUpperCase().equals("POST")){
//		  httpmethod=new PostMethod(uri);
//		}
//    
//		
//		if(queue != null && !queue.isEmpty()){
//			 val data=ArrayBuffer[NameValuePair]()
//			val it=queue.entrySet().iterator()
//			 while(it.hasNext()){
//			   val kv=it.next()
//			   println(kv.getValue())
//			   data.+=(new NameValuePair(kv.getKey,kv.getValue))
//			 }
//			 
// 			for(Entry<String, String> kv:queue.entrySet()){
// 				System.out.println(kv.getValue());
// 				data[i]=new NameValuePair(kv.getKey(), kv.getValue());
// 			}
// 			
//			if(MethodSignal.POST.equals(met)){
//				((PostMethod)method).setRequestBody(data);
//			}
//			else{
//				method.setQueryString(data);
//			}
//		}
//	
//		try {
//			System.out.println(method.getURI());
//			int status=client.executeMethod(method);
//			
//			if(status!=HttpStatus.SC_OK)
//				return msg=method.getStatusText();
//			
//			msg=method.getResponseBodyAsString();
//			
//		}catch (HttpException e){
//			System.err.println("Fatal protocol violation: " + e.getMessage());
//		      e.printStackTrace();
//		}catch (IOException e) {
//			System.err.println("Fatal protocol violation: " + e.getMessage());
//		      e.printStackTrace();
//		}finally{
//			method.releaseConnection();
//		}
//		return msg;
//	}
//  
//  
//}