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

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.view.Viewable;
import com.sun.jersey.spi.template.ViewProcessor;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;


import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * @author qiangwang@anjuke.com
 */
@Provider
public class VelocityViewProcessor implements ViewProcessor<String> {

	@Context
	private ThreadLocal<HttpServletRequest> request;

	private final Velocity v;

	public VelocityViewProcessor(@Context ResourceConfig resourceConfig, @Context ServletConfig sc) {
		v = new Velocity(sc);
	}

	@Override
	public String resolve(String path) {
		if(v.templateExists(path)){
			return path;
		}else{
			return null;
		}
	}

	@Override
	public void writeTo(String resolvedPath, Viewable viewable, OutputStream out)
			throws IOException {
		
		// Commit the status and headers to the HttpServletResponse
		out.flush();

		v.render(resolvedPath, request.get(), new OutputStreamWriter(out));
	}
}
