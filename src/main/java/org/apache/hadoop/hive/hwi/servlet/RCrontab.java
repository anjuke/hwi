package org.apache.hadoop.hive.hwi.servlet;

import java.net.URI;
import java.text.SimpleDateFormat;

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
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MCrontab.Status;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.Pagination;
import org.apache.hadoop.hive.hwi.query.QueryCron;
import org.apache.hadoop.hive.hwi.query.QueryStore;
import org.quartz.CronExpression;

import com.sun.jersey.api.view.Viewable;

@Path("/crontabs")
public class RCrontab extends RBase {
	protected static final Log l4j = LogFactory
			.getLog(RCrontab.class.getName());

	@GET
	@Produces("text/html")
	public Viewable list(
			@QueryParam(value = "page") @DefaultValue(value = "1") int page,
			@QueryParam(value = "pageSize") @DefaultValue(value = "20") int pageSize) {

		QueryStore qs = QueryStore.getInstance();

		Pagination<MCrontab> pagination = qs.crontabPaginate(page, pageSize);

		request.setAttribute("pagination", pagination);

		return new Viewable("/crontab/list.vm");
	}

	@GET
	@Path("{id}")
	@Produces("text/html")
	public Viewable info(@PathParam(value = "id") Integer id) {
		QueryStore qs = QueryStore.getInstance();

		MCrontab crontab = qs.getCrontabById(id);

		if (crontab == null)
			throw new WebApplicationException(404);

		request.setAttribute("crontab", crontab);

		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		request.setAttribute("createdTime", sf.format(crontab.getCreated()));
		request.setAttribute("updatedTime", sf.format(crontab.getUpdated()));

		return new Viewable("/crontab/info.vm");
	}

	@GET
	@Path("create")
	@Produces("text/html")
	public Viewable create(@QueryParam(value = "queryId") Integer queryId) {
		if (queryId != null) {
			MQuery mquery = QueryStore.getInstance().getById(queryId);
			if (mquery != null) {
				request.setAttribute("query", mquery.getQuery());
				request.setAttribute("callback", mquery.getCallback());
			}
		}
		return new Viewable("/crontab/create.vm");
	}

	@POST
	@Path("create")
	@Produces("text/html")
	public Viewable create(@FormParam(value = "name") String name,
			@FormParam(value = "query") String query,
			@FormParam(value = "callback") String callback,
			@FormParam(value = "crontab") String crontab) {

		Viewable v = new Viewable("/crontab/create.vm");
		request.setAttribute("name", name);
		request.setAttribute("query", query);
		request.setAttribute("callback", callback);
		request.setAttribute("crontab", crontab);

		if (name == null || name.equals("")) {
			request.setAttribute("msg", "name can't be empty");
			return v;
		}

		if (query == null || query.equals("")) {
			request.setAttribute("msg", "query can't be empty");
			return v;
		}

		if (crontab == null || crontab.equals("")) {
			request.setAttribute("msg", "crontab can't be empty");
			return v;
		}

		try {
			CronExpression.validateExpression(crontab);
		} catch (Exception e) {
			request.setAttribute("msg", "crontab: " + e.getMessage());
			return v;
		}

		QueryStore qs = QueryStore.getInstance();

		MCrontab mcrontab = new MCrontab(name, query, callback, crontab,
				"hadoop");
		qs.insertCrontab(mcrontab);

		QueryCron.getInstance().schedule(mcrontab);

		throw new WebApplicationException(Response.seeOther(
				URI.create("crontabs/" + mcrontab.getId())).build());
	}

	@GET
	@Path("{id}/changeStatus/{status}")
	@Produces("text/html")
	public Viewable pause(
			@PathParam(value = "id") int id, 
			@PathParam(value = "status") String status) {

		MCrontab crontab = QueryStore.getInstance().getCrontabById(id);
		if (crontab == null) {
			throw new WebApplicationException(404);
		}
		
		if (status == null) {
			throw new WebApplicationException(new Exception("status can't be empty"), 403);
		}

		Status s = null;
		
		try{
			s = Status.valueOf(status.toUpperCase());
		}catch(Exception e){
			throw new WebApplicationException(e, 403);
		}
		
		crontab.setStatus(s);
		QueryStore.getInstance().updateCrontab(crontab);

		throw new WebApplicationException(Response.seeOther(
				URI.create("crontabs/" + crontab.getId())).build());
	}

}
