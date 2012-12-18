<%--
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
--%>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page import="org.apache.hadoop.hive.ql.exec.Utilities"%>
<%@page errorPage="error_page.jsp" %>
<%
    String idStr = request.getParameter("id");
    Integer id = null;
    try {
        id = Integer.parseInt(idStr);
    } catch (Exception e) {
        
    }

	HiveConf hiveConf = new HiveConf(SessionState.class);
	QueryStore qs = new QueryStore(hiveConf);
	MQuery mquery = qs.getById(id);
	
	String[] jobIds = null;
	if(mquery.getJobId() != null){
		jobIds = mquery.getJobId().split(";");
	}
	
    String message = null;
    
%>
<!DOCTYPE html>
<html>
<head>
<title><%=mquery.getName()%></title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span2">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span2 -->
			<div class="span10">
				<h2><%=mquery.getName()%></h2>

				<% if (message != null) { %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>
			
			<dl class="dl-horizontal">
			<h3>Status</h3>
			<hr>
				<dt>Status</dt>
				<dd><%= mquery.getStatus() == null ? "--" : mquery.getStatus() %></dd>
			
				<dt>JobId</dt>
				<dd>
				<% if(mquery.getJobId() != null){ %>
				
				<% for(String jobId : jobIds){  %>
					<% if(!jobId.equals("")){ %>
					<a href="<%= HWIUtil.getJobTrackerURL(hiveConf, jobId) %>" target="_blank"><%= jobId %></a><br/>
					<% } %>
				<% } %>
				
				<% }else{ %>
					--
				<% } %>
				</dd>
				
			<h3>Basic</h3>
			<hr>
				<dt>Query</dt>
				<dd>
				<pre><%= mquery.getQuery() == null ? "--" : mquery.getQuery() %></pre>
				</dd>
				
				<dt>Callback</dt>
				<dd><code><%= mquery.getCallback() == null ? "--" : mquery.getCallback() %></code></dd>

				<dt>Result location</dt>
				<dd>
                <code><%= mquery.getResultLocation() == null ? "--" : mquery.getResultLocation() %></code>
                <% if (mquery.getStatus().equals(MQuery.Status.FINISHED)) { %>
                <a class="btn btn-small" href="query_result.jsp?id=<%= mquery.getId() %>" >View result</a>
                <% } %>
				</dd>
			
				<dt>Error message</dt>
				<dd><%= mquery.getErrorMsg() == null ? "--" : mquery.getErrorMsg() %></dd>
				
				<dt>Error code</dt>
                <dd><%= mquery.getErrorCode() == null ? "--" : mquery.getErrorCode() %></dd>
                
                <dt>Created</dt>
                <dd><%= mquery.getCreated() == null ? "--" : mquery.getCreated() %></dd>
                
                <dt>Updated</dt>
                <dd><%= mquery.getUpdated() == null ? "--" : mquery.getUpdated()%></dd>

			<h3>Stats</h3>
			<hr>
                <dt>Cpu Time</dt>
                <dd><%= mquery.getCpuTime() == null ? "--" : Utilities.formatMsecToStr(mquery.getCpuTime()) %></dd>
                
                <dt>Total Time</dt>
                <dd><%= mquery.getTotalTime() == null ? "--" : Utilities.formatMsecToStr(mquery.getTotalTime()) %></dd>
                
                <% if(mquery.getCpuTime() != null && mquery.getCpuTime() > 0 && mquery.getCpuTime() > mquery.getTotalTime() ){ %>
                <dt>Saved Time</dt>
                <dd><span class="badge badge-warning"><%= Utilities.formatMsecToStr(Math.abs(mquery.getCpuTime() - mquery.getTotalTime())) %></span></dd>
                <% } %>
			</dl>
				
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
