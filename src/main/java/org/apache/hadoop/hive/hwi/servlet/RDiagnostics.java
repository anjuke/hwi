/**
 * Copyright (C) [2013] [Anjuke Inc]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.hwi.servlet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.jersey.api.view.Viewable;

@Path("/diagnostics")
public class RDiagnostics extends RBase {
	protected static final Log l4j = LogFactory.getLog(RDiagnostics.class.getName());

	@GET
	@Produces("text/html")
	public Viewable dbs() {
		request.setAttribute("p", System.getProperties());
		request.setAttribute("env", System.getenv());

		return new Viewable("/diagnostics/info.vm");
	}
	
}
