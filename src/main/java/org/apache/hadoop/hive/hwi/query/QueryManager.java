package org.apache.hadoop.hive.hwi.query;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
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

public class QueryManager {
    protected static final Log l4j = LogFactory.getLog(QueryManager.class
            .getName());

    private static QueryManager instance;

    private Scheduler scheduler;
    private QueryMonitor monitor;

    private QueryManager() {
    }

    public static QueryManager getInstance() {
        if (instance == null) {
            synchronized (QueryManager.class) {
                if (instance == null) {
                    instance = new QueryManager();
                }
            }
        }
        return instance;
    }

    public void start() {
        try {
            l4j.info("QueryManager starting.");
            startScheduler();
            loadCrontabs();
            startMonitor();
            l4j.info("QueryManager started.");
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to start.");
        }
    }

    protected void startScheduler() throws SchedulerException {
        Properties props = new Properties();
        props.setProperty("org.quartz.threadPool.threadCount", "30");

        StdSchedulerFactory ssf = new StdSchedulerFactory(props);
        scheduler = ssf.getScheduler();
        scheduler.start();
    }

    protected void loadCrontabs() {
        List<MCrontab> crontabs = QueryStore.getInstance().runningCrontabs();
        for (MCrontab crontab : crontabs) {
            schedule(crontab);
        }
        l4j.info("Crontabs loaded.");
    }

    protected void startMonitor() {
        monitor = new QueryMonitor();
        monitor.start();
    }

    public boolean submit(MQuery mquery) {
        if (scheduler == null)
            return false;

        if (mquery.getId() == null)
            return false;

        JobDetail job = JobBuilder.newJob(QueryRunner.class)
                .withIdentity(mquery.getId().toString(), mquery.getUserId())
                .build();

        JobDataMap map = job.getJobDataMap();
        map.put("mqueryId", mquery.getId());

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(mquery.getId().toString(), mquery.getUserId())
                .startNow().build();

        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to schedule.");
            return false;
        }

        return true;
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
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to schedule.");
            return false;
        }

        return true;
    }

    public boolean unschedule(MCrontab crontab) {
        if (scheduler == null)
            return false;

        if (crontab.getId() == null)
            return false;

        try {
            scheduler.deleteJob(new JobKey(crontab.getId().toString(), crontab
                    .getUserId()));
            return true;
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager unschedule failed.");
            return false;
        }
    }

    public boolean monitor(QueryRunner runner) {
        if (monitor == null)
            return false;

        monitor.monitor(runner);
        return true;
    }

    public void shutdown() {
        l4j.info("QueryManager shutting down.");

        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                e.printStackTrace();
                l4j.error("scheduler failed to shutdown.");
            }
        }

        if (monitor != null)
            monitor.shutdown();

        l4j.info("QueryManager shutdown complete.");
    }

}
