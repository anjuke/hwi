package org.apache.hadoop.hive.hwi.servlet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.HWIUtil;
import org.apache.hadoop.hive.hwi.Pagination;
import org.apache.hadoop.hive.hwi.QueryManager;
import org.apache.hadoop.hive.hwi.QueryStore;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.sun.jersey.api.view.Viewable;

@Path("/queries")
public class Query extends Base {
	protected static final Log l4j = LogFactory.getLog(Query.class.getName());

	@GET
	@Produces("text/html")
	public Viewable list(
			@QueryParam(value = "page") @DefaultValue(value = "1") int page,
			@QueryParam(value = "pageSize") @DefaultValue(value = "20") int pageSize) {

		HiveConf hiveConf = new HiveConf(SessionState.class);
		QueryStore qs = new QueryStore(hiveConf);

		Pagination<MQuery> pagination = qs.paginate(page, pageSize);

		request.setAttribute("pagination", pagination);

		return new Viewable("/query/list.vm");
	}

	@GET
	@Path("{id}")
	@Produces("text/html")
	public Viewable info(@PathParam(value = "id") Integer id) {
		HiveConf hiveConf = new HiveConf(SessionState.class);
		QueryStore qs = new QueryStore(hiveConf);
		MQuery query = qs.getById(id);

		if (query == null)
			throw new WebApplicationException(404);

		request.setAttribute("query", query);

		if (query.getJobId() != null) {
			List<Map<String, Object>> jobInfos = new ArrayList<Map<String, Object>>();
			for (String jobId : query.getJobId().split(";")) {
				if (jobId.equals(""))
					continue;
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("id", jobId);
				map.put("url", HWIUtil.getJobTrackerURL(jobId));
				jobInfos.add(map);
			}
			request.setAttribute("jobInfos", jobInfos);
		}

		if (query.getCpuTime() != null) {
			request.setAttribute("cpuTime",
					Utilities.formatMsecToStr(query.getCpuTime()));
		}

		if (query.getTotalTime() != null) {
			request.setAttribute("toalTime",
					Utilities.formatMsecToStr(query.getTotalTime()));
		}

		if (query.getCpuTime() != null && query.getTotalTime() != null
				&& query.getCpuTime() > query.getTotalTime()) {
			request.setAttribute(
					"savedTime",
					Utilities.formatMsecToStr(Math.abs(query.getCpuTime()
							- query.getTotalTime())));
		}

		return new Viewable("/query/info.vm");
	}

	@GET
	@Path("create")
	@Produces("text/html")
	public Viewable create() {
		return new Viewable("/query/create.vm");
	}

	@POST
	@Path("create")
	@Produces("text/html")
	public Viewable create(@FormParam(value = "query") String query,
			@FormParam(value = "name") String name,
			@FormParam(value = "callback") String callback) {

		Viewable v = new Viewable("/query/create.vm");

		if (query == null || query.equals("")) {
			request.setAttribute("msg", "query can't be empty");
			return v;
		}

		Date created = Calendar.getInstance(TimeZone.getDefault()).getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

		if (name == null || "".equals(name)) {
			name = sf.format(created);
		}

		HiveConf hiveConf = new HiveConf(SessionState.class);
		QueryStore qs = new QueryStore(hiveConf);

		MQuery mquery = new MQuery();
		mquery.setName(name);
		mquery.setQuery(query);
		mquery.setCallback(callback);
		mquery.setResultLocation("");
		mquery.setCreated(created);
		mquery.setUpdated(created);
		mquery.setStatus(MQuery.Status.INITED);
		mquery.setResultLocation("");
		mquery.setUserId("hadoop");
		qs.insertQuery(mquery);

		mquery.setResultLocation("/user/hive/result/" + mquery.getId() + "/");
		qs.updateQuery(mquery);

		// submit to QueryManager
		QueryManager qm = (QueryManager) context.getAttribute("qm");
		qm.submit(mquery);

		throw new WebApplicationException(Response.seeOther(
				URI.create("queries/" + mquery.getId())).build());
	}

	@GET
	@Path("{id}/result")
	@Produces("text/html")
	public Viewable result(
			@PathParam(value = "id") Integer id,
			@QueryParam(value = "raw") @DefaultValue(value = "false") boolean raw) {
		Viewable v = new Viewable("/query/result.vm");

		HiveConf hiveConf = new HiveConf(SessionState.class);
		QueryStore qs = new QueryStore(hiveConf);
		MQuery query = qs.getById(id);

		if (query == null) {
			throw new WebApplicationException(404);
		}

		request.setAttribute("query", query);

		if (query.getStatus() != MQuery.Status.FINISHED) {
			throw new WebApplicationException(404);
		}

		ArrayList<String> partialResult = new ArrayList<String>();

		try {

			org.apache.hadoop.fs.Path rPath = new org.apache.hadoop.fs.Path(
					query.getResultLocation());
			FileSystem fs = rPath.getFileSystem(hiveConf);

			if (!fs.getFileStatus(rPath).isDir()) {
				request.setAttribute("msg", rPath + "is not directory");
				return v;
			}

			int readedLines = 0;
			String temp = null;

			FileStatus[] fss = fs.listStatus(rPath);
			for (FileStatus _fs : fss) {
				org.apache.hadoop.fs.Path _fsPath = _fs.getPath();
				if (!fs.getFileStatus(_fsPath).isDir()) {
					BufferedReader bf = new BufferedReader(
							new InputStreamReader(fs.open(_fsPath), "UTF-8"));

					if (raw) {
						response.addHeader("Content-Disposition",
								"attachment; filename=hwi_result_" + id
										+ ".txt");
						response.addHeader("Content-Type", "text/plain");
						PrintWriter writer = response.getWriter();
						while ((temp = bf.readLine()) != null) {
							writer.println(temp.replace('\1', '\t'));
						}
					} else {
						while ((temp = bf.readLine()) != null
								&& readedLines < 100) {
							partialResult.add(temp.replace('\1', '\t'));
							readedLines++;
						}

						if (readedLines >= 100) {
							break;
						}
					}

					bf.close();
				}
			}
			FileSystem.closeAll();
		} catch (Exception e) {
			throw new WebApplicationException(e);
		}

		if (raw) {
			throw new WebApplicationException(200);
		} else {
			request.setAttribute("partialResult", partialResult);
			return v;
		}
	}
}
