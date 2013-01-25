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

import java.util.HashMap;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Context;

import org.apache.commons.collections.map.HashedMap;

public abstract class RBase {
    
    public static final String USER_COOKIE_NAME = "user";

	@Context
	protected HttpServletRequest request;

	@Context
	protected HttpServletResponse response;
	
	/**
	 * set current user
	 * @param user
	 */
	protected void setUser(String user) {
	    Cookie cookie = new Cookie(USER_COOKIE_NAME, user);
	    cookie.setMaxAge(365 * 24 * 60 * 60);
	    cookie.setPath("/hwi");
	    response.addCookie(cookie);
	}
	
	/**
	 * return current user
	 * 
	 * @return
	 */
	protected String getUser() {
	    try {
	        String user = getCookies().get(USER_COOKIE_NAME);
	        return user;
	    } catch (Exception e) {
	        return null;
	    }
	}
	
	/**
	 * return user cookies
	 * 
	 * @return user cookies hashmap
	 */
	protected HashMap<String, String> getCookies() {
	    Cookie _cookies[] =  request.getCookies();
	    
	    HashMap<String, String> cookies = new HashMap<String, String>();
	    for (int i=0; i<_cookies.length; i++) {
	        cookies.put(_cookies[i].getName(), _cookies[i].getValue());
	    }
	    
	    return cookies;	    
	}
	
}
