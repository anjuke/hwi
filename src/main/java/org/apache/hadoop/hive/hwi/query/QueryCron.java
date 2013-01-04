package org.apache.hadoop.hive.hwi.query;

import java.util.List;

import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class QueryCron {
	protected static final Log l4j = LogFactory.getLog(QueryCron.class
			.getName());

	private static QueryCron instance;

	private Scheduler scheduler;

	private QueryCron() {
	}

	public static QueryCron getInstance() {
		if (instance == null) {
			synchronized (QueryCron.class) {
				if (instance == null) {
					instance = new QueryCron();
				}
			}
		}
		return instance;
	}

	public void start(){
		try {
			startScheduler();
			loadCrontabs();
			l4j.error("QueryCron start.");
		} catch (SchedulerException e) {
			e.printStackTrace();
			l4j.error("QueryCron start failed.");
		}
	}
	
	protected void startScheduler() throws SchedulerException {
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		scheduler.start();
		l4j.error("Scheduler started.");
	}

	protected void loadCrontabs() {
		List<MCrontab> crontabs = QueryStore.getInstance().runningCrontabs();
		for (MCrontab crontab : crontabs) {
			schedule(crontab);
		}
		l4j.error("Crontabs loaded.");
	}

	public boolean schedule(MCrontab crontab) {
		if (scheduler == null)
			return false;

		if (crontab.getId() == null)
			return false;

		JobDetail job = JobBuilder.newJob(QueryCrontab.class)
				.withIdentity(crontab.getId().toString(), crontab.getUserId())
				.build();

		JobDataMap map = job.getJobDataMap();
		map.put("crontabId", crontab.getId());

		Trigger trigger = TriggerBuilder
				.newTrigger()
				.withIdentity(crontab.getId().toString(), crontab.getUserId())
				.withSchedule(
						CronScheduleBuilder.cronSchedule(crontab.getCrontab()))
				.build();

		try {
			scheduler.scheduleJob(job, trigger);
			return true;
		} catch (SchedulerException e) {
			e.printStackTrace();
			l4j.error("QueryCron schedule failed.");
			return false;
		}

	}


	public boolean unschedule(MCrontab crontab) {
		if (scheduler == null)
			return false;

		if (crontab.getId() == null)
			return false;
		
		try {
			scheduler.deleteJob(new JobKey(crontab.getId().toString(), crontab.getUserId()));
			return true;
		} catch (SchedulerException e) {
			e.printStackTrace();
			l4j.error("QueryCron unschedule failed.");
			return false;
		}
	}
	
	public void shutdown() {
		if (scheduler == null)
			return;

		try {
			scheduler.shutdown();
			l4j.error("QueryCron shutdown.");
		} catch (SchedulerException e) {
			e.printStackTrace();
			l4j.error("QueryCron shutdown failed.");
		}
	}

}
