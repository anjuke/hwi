package org.apache.hadoop.hive.hwi.servlet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.jersey.api.view.Viewable;

@Path("/")
public class RIndex extends RBase {
	protected static final Log l4j = LogFactory.getLog(RIndex.class
		      .getName());
	 
    @GET
    @Produces("text/html")
    public Viewable index() {
    	return new Viewable("/index.vm");
    }

}
