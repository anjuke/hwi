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

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.sun.jersey.api.view.Viewable;

@Path("/schema")
public class RSchema extends RBase {
	protected static final Log l4j = LogFactory.getLog(RSchema.class.getName());

	@GET
	@Produces("text/html")
	public Viewable dbs() {
		HiveConf hiveConf = new HiveConf(SessionState.class);
		try {
			HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
			List<String> dbs = client.getAllDatabases();
			client.close();
			request.setAttribute("dbs", dbs);
		} catch (MetaException e) {
			throw new WebApplicationException(e);
		}

		return new Viewable("/schema/dbs.vm");
	}

	@GET
	@Path("{name}")
	@Produces("text/html")
	public Viewable db(@PathParam(value = "name") String name) {
		HiveConf hiveConf = new HiveConf(SessionState.class);
		try {
			HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
			Database db = client.getDatabase(name);
			List<String> tables = client.getAllTables(name);
			client.close();
			request.setAttribute("db", db);
			request.setAttribute("tables", tables);
		} catch (Exception e) {
			throw new WebApplicationException(e);
		}

		return new Viewable("/schema/db.vm");
	}

	@GET
	@Path("{dbName}/{tableName}")
	@Produces("text/html")
	public Viewable table(@PathParam(value = "dbName") String dbName,
			@PathParam(value = "tableName") String tableName) {
		HiveConf hiveConf = new HiveConf(SessionState.class);
		try {
			HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
			Table t = client.getTable(dbName, tableName);
			client.close();
			request.setAttribute("t", t);
		} catch (Exception e) {
			throw new WebApplicationException(e);
		}

		return new Viewable("/schema/table.vm");
	}

}
