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
	
	String name = mquery.getName();
	String query = mquery.getQuery();
	String resultLocation = mquery.getResultLocation();
	String callback = mquery.getCallback();
	
    String message = null;
    String action = request.getParameter("action");
    String errmsg = mquery.getErrorMsg();
    
    MQuery.Status status = mquery.getStatus();
    
    if ("update_query".equals(action)) {
        name = request.getParameter("name");
        query = request.getParameter("query");
        resultLocation = request.getParameter("resultLocation");
        callback = request.getParameter("callback");
        //out.println(mquery.getId());
        
        mquery.setName(name);
        mquery.setQuery(query);
        mquery.setResultLocation(resultLocation);
        mquery.setCallback(callback);
        qs.updateQuery(mquery);
    }
%>
<!DOCTYPE html>
<html>
<head>
<title>Manage Query <%=name%></title>
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
				<h2>Manage Query <%=name%></h2>

				<% if (message != null) {  %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>

                <% if (status == MQuery.Status.RUNNING) { %>
				<div class="alert alert-warning">
				Session is in QUERY_RUNNING state. Changes are not possible!
				</div>
				<% } %>
				
				<%-- 
          	View JobTracker: <a href="<%= sess.getJobTrackerURI() %>">View Job</a><br>
          	Kill Command: <%= sess.getKillCommand() %>
          	 Session Kill: <a href="/hwi/session_kill.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br>
          	--%>

				<div class="btn-group">
					<a class="btn" href="/hwi/query_diagnostics.jsp?id=<%=id%>"><i class="icon-cog"></i> Diagnostics</a>
					<a class="btn" href="/hwi/query_manage.jsp?action=remove&id=<%=id%>"><i class="icon-remove"></i> Remove</a>
					<a class="btn" href="/hwi/query_result.jsp?id=<%=id%>"><i class=" icon-download-alt"></i> Result Bucket</a>
				</div>

				<form action="" method="post" class="form-horizontal">

					<fieldset>
						<legend>Query Details	</legend>
						
						<div class="control-group">
                            <label class="control-label" for="fldname">Name</label>
                            <div class="controls">
                                <input id="fldname" type="text" name="name" value="<%= name %>" />
                            </div>
                        </div>
                        
                        <div class="control-group">
                            <label class="control-label" for="fldquery">Query</label>
                            <div class="controls">
                                <textarea id="fldquery" name="query"><%= query %></textarea>
	                            <% if (errmsg != null && !"".equals(errmsg)) { %>
                            </div>
                        </div>
						
						<div class="control-group">
							<label class="control-label" for="fldresultLocation">Result Location</label>
							<div class="controls">
								<input id="fldresultLocation" type="text" name="resultLocation" value="<%= resultLocation %>" />
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="flderrfile">Error File</label>
							<div class="controls">
								<input id="flderrfile" type="text" name="errorFile" value="" />
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="fldcallback">Callback</label>
							<div class="controls">
								<input id="fldcallback" name="callback" value="<%= callback %>" />
							</div>
						</div>

					</fieldset>

					<h3>Query Return Message</h3>
					<p>
					</p>
					<div class="alert alert-error">
                    Error : <%= errmsg %>
                    </div>
                    <% } %>

					<div class="form-actions">
					    <input type="hidden" name="action" value="update_query" />
						<button type="submit" class="btn btn-primary">Submit</button>
					</div>
					
				</form>
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>