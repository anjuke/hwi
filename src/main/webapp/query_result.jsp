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
--%><%@page 
import="java.io.InputStreamReader,
java.io.BufferedReader,
java.io.BufferedInputStream,
org.apache.hadoop.fs.Path,
org.apache.hadoop.fs.FileSystem,
org.apache.hadoop.hive.hwi.*,
org.apache.hadoop.hive.hwi.model.MQuery,
org.apache.hadoop.hive.conf.HiveConf,
org.apache.hadoop.fs.FSDataInputStream,
org.apache.hadoop.hive.ql.session.SessionState,
org.apache.hadoop.fs.FileStatus" 
errorPage="error_page.jsp"
pageEncoding="UTF-8" 
%><%
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

    String message = null;
    String errmsg = mquery.getErrorMsg();

    MQuery.Status status = mquery.getStatus();

    String temp, resultHtml = "";
    if (status == MQuery.Status.FINISHED) {
        boolean isdownload = "download".equals(request
                .getParameter("action"));

        Path rPath = new Path(resultLocation);
        FileSystem fs = rPath.getFileSystem(hiveConf);

        if (isdownload) {
            response.addHeader("Content-Disposition", "attachment; filename=hwi_result_" + id + ".txt");
            response.addHeader("Content-Type", "text/plain");
        }

        int readedLines = 0;
        if (fs.getFileStatus(rPath).isDir()) {
            FileStatus[] fss = fs.listStatus(rPath);
            for (FileStatus _fs : fss) {
                Path _fsPath = _fs.getPath();
                if (!fs.getFileStatus(_fsPath).isDir()) {
                    BufferedReader bf = new BufferedReader(
                            new InputStreamReader(fs.open(_fsPath), "UTF-8"));

                    if (isdownload) {
                        while ((temp = bf.readLine()) != null) {
                            out.println(temp.replace('\1', '\t'));
                        }
                    } else {
                        resultHtml += ("<h5>" + _fsPath + "</h5><code>");

                        while ((temp = bf.readLine()) != null) {
                            resultHtml += (temp.replace('\1', '\t') + "<br />\n");
                            if (++readedLines >= 1000) {
                                break;
                            }
                        }

                        if ((temp = bf.readLine()) != null) {
                            resultHtml += "<p><a class=\"btn btn-small\" href=\"?id=" + id + "&action=download\"><i class=\"icon-plus\"></i> Full result</a></p>";
                        }
                        resultHtml += "</code>";
                    }

                    bf.close();
                }
            }

            // download no need output html
            if (isdownload) {
                return;
            }
        } else {
            resultHtml = "Cannot read file " + rPath;
        }
        FileSystem.closeAll();
    } else {
        resultHtml = "Query is not in FINISHED status. Result are not exists!";
    }
%><!DOCTYPE html>
<html>
<head>
<title>Query Result <%=name%></title>
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
				<h4><%=name%> Query Result</h4>
                <i class="icon-arrow-left"></i> <a href="query_info.jsp?id=<%= id %>">Query Info</a>  <i class="icon-download-alt"></i> <a href="?id=<%= id %>&action=download">Download</a>

				<% if (message != null) {  %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>

                
                <div <% if (status != MQuery.Status.FINISHED) { %> class="alert alert-warning"<% } %>>
                <%= resultHtml %>
                </div>
				
			</div><!-- span10 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>