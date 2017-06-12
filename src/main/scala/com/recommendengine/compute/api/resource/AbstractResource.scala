package com.recommendengine.compute.api.resource

import org.restlet.Context

import com.recommendengine.compute.api.RecServer

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

trait AbstractResource {
    
   protected val server:RecServer=Context.getCurrent.getAttributes.get(RecServer.SERVER_NAME).asInstanceOf[RecServer]
   
   protected val tmr=server.tmr
   
   protected val mqmr=server.mqmr
   
   def throwBadRequestException(msg:String)=throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(msg).build())
   
}