package org.apache.hadoop.hive.hwi.servlet;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;

public abstract class Base {
	@Context
	protected ServletContext context;
	
	@Context
	protected HttpServletRequest request;

	@Context
	protected HttpServletResponse response;
}
