package org.apache.hadoop.hive.hwi.model;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class MCrontab {

	public static enum Status {
		RUNNING, PAUSED, DELETED
	};
	
	private Integer id;

	private String name;

	private String query;

	private String callback;

	private String userId;
	
	private String crontab;

	private Date created;

	private Date updated;
	
	private Status status;
	
	public MCrontab(String name, String query, String callback, String crontab, String userId) {
		this.name = name;
		this.query = query;
		this.callback = callback;
		this.crontab = crontab;
		this.userId = userId;
		this.created = Calendar.getInstance(TimeZone.getDefault()).getTime();
		this.updated = this.created;
		this.setStatus(Status.PAUSED);
	}
	

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getCallback() {
		return callback;
	}

	public void setCallback(String callback) {
		this.callback = callback;
	}

	public String getCrontab() {
		return crontab;
	}

	public void setCrontab(String crontab) {
		this.crontab = crontab;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getUpdated() {
		return updated;
	}

	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

}
