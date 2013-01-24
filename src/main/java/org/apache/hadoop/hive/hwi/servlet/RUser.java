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

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.sun.jersey.api.view.Viewable;

@Path("/users")
public class RUser extends RBase {
    
    @Path("set")
    @Produces("text/html;charset=ISO-8859-1")
    @GET
    public Viewable set() {
        request.setAttribute("user", getUser());
        return new Viewable("/user/set.vm");
    }
    
    @Path("set")
    @Produces("text/html;charset=ISO-8859-1")
    @POST
    public Viewable set(@FormParam(value = "user") String user) {
        setUser(user);
        request.setAttribute("msg", "set user successful!");
        request.setAttribute("msg-type", "success");
        return new Viewable("/user/set.vm");
    }

}
