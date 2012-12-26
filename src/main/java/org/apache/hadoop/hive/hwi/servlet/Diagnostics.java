package org.apache.hadoop.hive.hwi.servlet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.jersey.api.view.Viewable;

@Path("/diagnostics")
public class Diagnostics extends Base {
	protected static final Log l4j = LogFactory.getLog(Diagnostics.class.getName());

	@GET
	@Produces("text/html")
	public Viewable dbs() {
		request.setAttribute("p", System.getProperties());
		request.setAttribute("env", System.getenv());

		return new Viewable("/diagnostics/info.vm");
	}
	
}
